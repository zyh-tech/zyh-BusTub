//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/disk/hash/disk_extendible_hash_table.h"

namespace bustub {


template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                         const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  HashTableDirectoryPage *dir_page =
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
  dir_page->SetPageId(directory_page_id_);
  page_id_t new_bucket_id;
  buffer_pool_manager_->NewPage(&new_bucket_id);
  dir_page->SetBucketPageId(0, new_bucket_id);
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
//计算key对应的32位二进制数字
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Hash(KeyType key) -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

//求出key应该存储到的目录项
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hashed_key = Hash(key);
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return mask & hashed_key;
}

//求出key应该存储到的桶的页面id
template <typename KeyType, typename ValueType, typename KeyComparator>
page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t idx = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(idx);
}

//获取目录页面内容
template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
}

//获取桶页面内容
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
//GetValue从哈希表中读取与键匹配的所有值结果，其通过哈希表的读锁保护目录页面，
//并使用桶的读锁保护桶页面。具体的操作步骤为先读取目录页面，再通过目录页面和哈希键或许对应的桶页面，
//最后调用桶页面的GetValue获取值结果。在函数返回时注意要UnpinPage所获取的页面。
//加锁时应当保证锁的获取、释放全局顺序以避免死锁。
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->RLatch();
  bool ret = bucket->GetValue(key, comparator_, result);
  p->RUnlatch();
  table_latch_.RUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));

  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->WLatch();
  if (bucket->IsFull()) {
    p->WUnlatch();
    table_latch_.RUnlock();
    assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
    assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
    return SplitInsert(transaction, key, value);
  }
  bool ret = bucket->Insert(key, value, comparator_);
  p->WUnlatch();
  table_latch_.RUnlock();
  // std::cout<<"find the unfull bucket"<<std::endl;
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  return ret;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  while (true) {
    page_id_t bucket_page_id = KeyToPageId(key, dir_page);
    uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
    HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
    if (bucket->IsFull()) {
      uint32_t global_depth = dir_page->GetGlobalDepth();
      uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
      page_id_t new_bucket_id = 0;
      HASH_TABLE_BUCKET_TYPE *new_bucket =
          reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
      assert(new_bucket != nullptr);
      if (global_depth == local_depth) {
        // if i == ij, extand the bucket dir, and split the bucket
        uint32_t bucket_num = 1 << global_depth;
        for (uint32_t i = 0; i < bucket_num; i++) {
          dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));
          dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));
        }
        dir_page->IncrGlobalDepth();
        dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);
        dir_page->IncrLocalDepth(bucket_idx);
        dir_page->IncrLocalDepth(bucket_idx + bucket_num);
        global_depth++;
      } else {
        // if i > ij, split the bucket
        // more than one records point to the bucket
        // the records' low ij bits are same
        // and the high (i - ij) bits are index of the records point to the same bucket
        uint32_t mask = (1 << local_depth) - 1;
        uint32_t base_idx = mask & bucket_idx;
        uint32_t records_num = 1 << (global_depth - local_depth - 1);
        uint32_t step = (1 << local_depth);
        uint32_t idx = base_idx;
        for (uint32_t i = 0; i < records_num; i++) {
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }
        idx = base_idx + step;
        for (uint32_t i = 0; i < records_num; i++) {
          dir_page->SetBucketPageId(idx, new_bucket_id);
          dir_page->IncrLocalDepth(idx);
          idx += step * 2;
        }
      }
      // dir_page->PrintDirectory();
      // rehash all records in bucket j
      for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
        KeyType j_key = bucket->KeyAt(i);
        ValueType j_value = bucket->ValueAt(i);
        bucket->RemoveAt(i);
        if (KeyToPageId(j_key, dir_page) == bucket_page_id) {
          bucket->Insert(j_key, j_value, comparator_);
        } else {
          new_bucket->Insert(j_key, j_value, comparator_);
        }
      }
      // std::cout<<"original bucket size = "<<bucket->NumReadable()<<std::endl;
      // std::cout<<"new bucket size = "<<new_bucket->NumReadable()<<std::endl;
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
      assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
    } else {
      bool ret = bucket->Insert(key, value, comparator_);
      table_latch_.WUnlock();
      // std::cout<<"find the unfull bucket"<<std::endl;
      assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
      assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
      return ret;
    }
  }

  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) -> bool {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.RLock();
  page_id_t bucket_page_id = KeyToPageId(key, dir_page);
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket);
  p->WLatch();
  bool ret = bucket->Remove(key, value, comparator_);
  p->WUnlatch();
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
    table_latch_.RUnlock();
    this->Merge(transaction, key, value);
  } else {
    table_latch_.RUnlock();
  }
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  return ret;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  table_latch_.WLock();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
  if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
    uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
    uint32_t global_depth = dir_page->GetGlobalDepth();
    // How to find the bucket to Merge?
    // Answer: After Merge, the records, which pointed to the Merged Bucket,
    // have low (local_depth - 1) bits same
    // therefore, reverse the low local_depth can get the idx point to the bucket to Merge
    uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
    page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);
    HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);
    if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) {
      local_depth--;
      uint32_t mask = (1 << local_depth) - 1;
      uint32_t idx = mask & bucket_idx;
      uint32_t records_num = 1 << (global_depth - local_depth);
      uint32_t step = (1 << local_depth);

      for (uint32_t i = 0; i < records_num; i++) {
        dir_page->SetBucketPageId(idx, bucket_page_id);
        dir_page->DecrLocalDepth(idx);
        idx += step;
      }
      buffer_pool_manager_->DeletePage(merged_page_id);
    }
    if (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
    assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));
  }
  table_latch_.WUnlock();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
  assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
}


/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetGlobalDepth() -> uint32_t {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class DiskExtendibleHashTable<int, int, IntComparator>;

template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
