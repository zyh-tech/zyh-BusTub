//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//Extendible Hash Table 由一个 directory 和多个 bucket 组成。

//directory: 存放指向 bucket 的指针，是一个数组。
//用于寻找 key 对应 value 所在的 bucket。
//bucket: 存放 value，是一个链表。一个 bucket 可以至多存放指定数量的 value。

/*
Extendible Hash Table 与 Chained Hash Table 最大的区别是，
Extendible Hash 中，不同的指针可以指向同一个 bucket，
而 Chained Hash 中每个指针对应一个 bucket。
Extendible Hash 中，如果 bucket 到达容量上限，则对桶会进行一次 split 操作

Extendible Hash 的插入流程。
将一个键值对 (K,V) 插入哈希表时，会先用哈希函数计算 K 的哈希值 H(K)，
并用此哈希值计算出索引，将 V 放入索引对应的 bucket 中。
Extendible Hash 计算索引的方式是直接取哈希值 H(K) 的低 n 位。

当需要插入 K 时，发现 bucket 已满，首先判断当前 bucket 的 local depth 是否等于 global depth：
若相等，即仅有一个指针指向 bucket，需要对 dir 扩容。
若不相等，即有多个指针指向 bucket，则无需扩容，将原来指向此 bucket 的指针重新分配。

*/


//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  return target_bucket->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);

  while (dir_[IndexOf(key)]->IsFull()) {
    auto index = IndexOf(key);
    auto target_bucket = dir_[index];

    if (target_bucket->GetDepth() == GetGlobalDepthInternal()) {
      //桶的深度等于全局深度，需要扩容
      /*
          global depth++
          directory 容量翻倍
          创建一个新的 bucket
          重新安排指针
          重新分配 KV 对
      */
      global_depth_++;
      int capacity = dir_.size();
      dir_.resize(capacity << 1);
      for (int i = 0; i < capacity; i++) {
        dir_[i + capacity] = dir_[i];
      }
    }

    int mask = 1 << target_bucket->GetDepth();
    auto bucket_0 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);
    auto bucket_1 = std::make_shared<Bucket>(bucket_size_, target_bucket->GetDepth() + 1);

    for (const auto &item : target_bucket->GetItems()) {
      size_t hash_key = std::hash<K>()(item.first);
      if ((hash_key & mask) != 0U) {
        bucket_1->Insert(item.first, item.second);
      } else {
        bucket_0->Insert(item.first, item.second);
      }
    }

    //桶分裂
    num_buckets_++;

    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == target_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket_1;
        } else {
          dir_[i] = bucket_0;
        }
      }
    }
  }

  auto index = IndexOf(key);
  auto target_bucket = dir_[index];

  for (auto &item : target_bucket->GetItems()) {
    if (item.first == key) {
      item.second = value;
      return;
    }
  }

  target_bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &item) {
    if (item.first == key) {
      value = item.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](const auto &item) {
    if (item.first == key) {
      this->list_.remove(item);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
