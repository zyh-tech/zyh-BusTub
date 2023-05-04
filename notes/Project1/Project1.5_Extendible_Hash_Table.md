## Task 5 : HASH TABLE IMPLEMENTATION + CONCURRENCY CONTROL

在这两个部分中，我们需要实现一个线程安全的可扩展哈希表。将其实现并不困难，难点在于如何在降低锁粒度、提高并发性的情况下保证线程安全。

```C++
 24 template <typename KeyType, typename ValueType, typename KeyComparator>
 25 HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
 26                                      const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
 27     : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
 28   // LOG_DEBUG("BUCKET_ARRAY_SIZE = %ld", BUCKET_ARRAY_SIZE);
 29   HashTableDirectoryPage *dir_page =
 30       reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_));
 31   dir_page->SetPageId(directory_page_id_);
 32   page_id_t new_bucket_id;
 33   buffer_pool_manager_->NewPage(&new_bucket_id);
 34   dir_page->SetBucketPageId(0, new_bucket_id);
 35   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
 36   assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
 37 }
```

在构造函数中，为哈希表分配一个目录页面和桶页面，并设置目录页面的`page_id`成员、将哈希表的首个目录项指向该桶。最后，不要忘记调用`UnpinPage`向缓冲池告知页面的使用完毕。

```C++
 54 template <typename KeyType, typename ValueType, typename KeyComparator>
 55 uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
 56   uint32_t hashed_key = Hash(key);
 57   uint32_t mask = dir_page->GetGlobalDepthMask();
 58   return mask & hashed_key;
 59 }
 60 
 61 template <typename KeyType, typename ValueType, typename KeyComparator>
 62 page_id_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
 63   uint32_t idx = KeyToDirectoryIndex(key, dir_page);
 64   return dir_page->GetBucketPageId(idx);
 65 }
 66 
 67 template <typename KeyType, typename ValueType, typename KeyComparator>
 68 HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
 69   return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_));
 70 }
 71 
 72 template <typename KeyType, typename ValueType, typename KeyComparator>
 73 HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
 74   return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->FetchPage(bucket_page_id));
 75 }
 76 
```

上面是一些用于提取目录页面、桶页面以及目录页面中的目录项的功能函数。

```C++
 80 template <typename KeyType, typename ValueType, typename KeyComparator>
 81 bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
 82   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
 83   table_latch_.RLock();
 84   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
 85   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
 86   Page *p = reinterpret_cast<Page *>(bucket);
 87   p->RLatch();
 88   bool ret = bucket->GetValue(key, comparator_, result);
 89   p->RUnlatch();
 90   table_latch_.RUnlock();
 91   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
 92   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, false, nullptr));
 93 
 94   return ret;
 95 }
```

`GetValue`从哈希表中读取与键匹配的所有值结果，其通过哈希表的读锁保护目录页面，并使用桶的读锁保护桶页面。具体的操作步骤为先读取目录页面，再通过目录页面和哈希键或许对应的桶页面，最后调用桶页面的`GetValue`获取值结果。在函数返回时注意要`UnpinPage`所获取的页面。加锁时应当保证锁的获取、释放全局顺序以避免死锁。

```C++
100 template <typename KeyType, typename ValueType, typename KeyComparator>
101 bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value    ) {
102   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
103   table_latch_.RLock();
104   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
105   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
106   Page *p = reinterpret_cast<Page *>(bucket);
107   p->WLatch();
108   if (bucket->IsFull()) {
109     p->WUnlatch();
110     table_latch_.RUnlock();
111     assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
112     assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
113     return SplitInsert(transaction, key, value);
114   }
115   bool ret = bucket->Insert(key, value, comparator_);
116   p->WUnlatch();
117   table_latch_.RUnlock();
118   // std::cout<<"find the unfull bucket"<<std::endl;
119   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
120   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
121   return ret;
122 }
```

`Insert`向哈希表插入键值对，这可能会导致桶的分裂和表的扩张，因此需要保证目录页面的读线程安全，一种比较简单的保证线程安全的方法为：在操作目录页面前对目录页面加读锁。但这种加锁方式使得`Insert`函数阻塞了整个哈希表，这严重影响了哈希表的并发性。可以注意到，表的扩张的发生频率并不高，对目录页面的操作属于读多写少的情况，因此可以使用乐观锁的方法优化并发性能，其在`Insert`被调用时仅保持读锁，只在需要桶分裂时重新获得读锁。

`Insert`函数的具体流程为：

1. 获取目录页面和桶页面，在加全局读锁和桶写锁后检查桶是否已满，如已满则释放锁，并调用`UnpinPage`释放页面，然后调用`SplitInsert`实现桶分裂和插入；
2. 如当前桶未满，则直接向该桶页面插入键值对，并释放锁和页面即可。

```C++
124 template <typename KeyType, typename ValueType, typename KeyComparator>
125 bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
126   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
127   table_latch_.WLock();
128   while (true) {
129     page_id_t bucket_page_id = KeyToPageId(key, dir_page);
130     uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
131     HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
132     if (bucket->IsFull()) {
133       uint32_t global_depth = dir_page->GetGlobalDepth();
134       uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
135       page_id_t new_bucket_id = 0;
136       HASH_TABLE_BUCKET_TYPE *new_bucket =
137           reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(buffer_pool_manager_->NewPage(&new_bucket_id));
138       assert(new_bucket != nullptr);
```

由于`SplitInsert`比较复杂
**124-138行**：首先，获取目录页面并加全局写锁，在添加全局写锁后，其他所有线程均被阻塞了，因此可以放心的操作数据成员。不难注意到，在`Insert`中释放读锁和`SplitInsert`中释放写锁间存在空隙，其他线程可能在该空隙中被调度，从而改变桶页面或目录页面数据。因此，在这里需要重新在目录页面中获取哈希键所对应的桶页面（可能与`Insert`中判断已满的页面不是同一页面），并检查对应的桶页面是否已满。如桶页面仍然是满的，则分配新桶和提取原桶页面的元数据。在由于桶分裂后仍所需插入的桶仍可能是满的，因此在这这里进行循环以解决该问题。

```C++
139       if (global_depth == local_depth) {
140         // if i == ij, extand the bucket dir, and split the bucket
141         uint32_t bucket_num = 1 << global_depth;
142         for (uint32_t i = 0; i < bucket_num; i++) {
143           dir_page->SetBucketPageId(i + bucket_num, dir_page->GetBucketPageId(i));
144           dir_page->SetLocalDepth(i + bucket_num, dir_page->GetLocalDepth(i));
145         } 
146         dir_page->IncrGlobalDepth();
147         dir_page->SetBucketPageId(bucket_idx + bucket_num, new_bucket_id);
148         dir_page->IncrLocalDepth(bucket_idx);
149         dir_page->IncrLocalDepth(bucket_idx + bucket_num);
150         global_depth++;
151       } else {
152         // if i > ij, split the bucket
153         // more than one records point to the bucket
154         // the records' low ij bits are same
155         // and the high (i - ij) bits are index of the records point to the same bucket
156         uint32_t mask = (1 << local_depth) - 1;
157         uint32_t base_idx = mask & bucket_idx;
158         uint32_t records_num = 1 << (global_depth - local_depth - 1);
159         uint32_t step = (1 << local_depth);
160         uint32_t idx = base_idx;
161         for (uint32_t i = 0; i < records_num; i++) {
162           dir_page->IncrLocalDepth(idx);
163           idx += step * 2;
164         } 
165         idx = base_idx + step;
166         for (uint32_t i = 0; i < records_num; i++) {
167           dir_page->SetBucketPageId(idx, new_bucket_id);
168           dir_page->IncrLocalDepth(idx); 
169           idx += step * 2;
170         }
171       }
```

**139-171行**：在这里，需要根据全局深度和桶页面的局部深度判断扩展表和分裂桶的策略。当`global_depth == local_depth`时，需要进行表扩展和桶分裂，`global_depth == local_depth`仅需进行桶分裂即可。

```C++
173       // rehash all records in bucket j
174       for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
175         KeyType j_key = bucket->KeyAt(i);
176         ValueType j_value = bucket->ValueAt(i);
177         bucket->RemoveAt(i);
178         if (KeyToPageId(j_key, dir_page) == bucket_page_id) {
179           bucket->Insert(j_key, j_value, comparator_);
180         } else {
181           new_bucket->Insert(j_key, j_value, comparator_);
182         }
183       }
184       // std::cout<<"original bucket size = "<<bucket->NumReadable()<<std::endl;
185       // std::cout<<"new bucket size = "<<new_bucket->NumReadable()<<std::endl;
186       assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
187       assert(buffer_pool_manager_->UnpinPage(new_bucket_id, true, nullptr));
```

**173-187行**：在完成桶分裂后，应当将原桶页面中的记录重新插入哈希表，由于记录的低`i-1`位仅与原桶页面和新桶页面对应，因此记录插入的桶页面仅可能为原桶页面和新桶页面两个选择。在重新插入完记录后，释放新桶页面和原桶页面。

```C++
188     } else {
189       bool ret = bucket->Insert(key, value, comparator_);
190       table_latch_.WUnlock();
191       // std::cout<<"find the unfull bucket"<<std::endl;
192       assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
193       assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
194       return ret;
195     }
196   }
197 
198   return false;
199 }
```

**188-198行**：若当前键值对所插入的桶页面非空（被其他线程修改或桶分裂后结果），则直接插入键值对，并释放锁和页面，并将插入结果返回`Insert`。

```C++
204 template <typename KeyType, typename ValueType, typename KeyComparator>
205 bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
206   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
207   table_latch_.RLock();
208   page_id_t bucket_page_id = KeyToPageId(key, dir_page);
209   uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
210   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
211   Page *p = reinterpret_cast<Page *>(bucket);
212   p->WLatch();
213   bool ret = bucket->Remove(key, value, comparator_);
214   p->WUnlatch();
215   if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
216     table_latch_.RUnlock();
217     this->Merge(transaction, key, value);
218   } else {
219     table_latch_.RUnlock();
220   }
221   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
222   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
223   return ret;
224 }
```

`Remove`从哈希表中删除对应的键值对，其优化思想与`Insert`相同，由于桶的合并并不频繁，因此在删除键值对时仅获取全局读锁，只在需要合并桶时获取全局写锁。当删除后桶为空且目录项的局部深度不为零时，释放读锁并调用`Merge`尝试合并页面，随后释放锁和页面并返回。

```C++
229 template <typename KeyType, typename ValueType, typename KeyComparator>
230 void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
231   HashTableDirectoryPage *dir_page = FetchDirectoryPage();
232   table_latch_.WLock();
233   uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
234   page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
235   HASH_TABLE_BUCKET_TYPE *bucket = FetchBucketPage(bucket_page_id);
236   if (bucket->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) != 0) {
237     uint32_t local_depth = dir_page->GetLocalDepth(bucket_idx);
238     uint32_t global_depth = dir_page->GetGlobalDepth();
239     // How to find the bucket to Merge?
240     // Answer: After Merge, the records, which pointed to the Merged Bucket,
241     // have low (local_depth - 1) bits same
242     // therefore, reverse the low local_depth can get the idx point to the bucket to Merge
243     uint32_t merged_bucket_idx = bucket_idx ^ (1 << (local_depth - 1));
244     page_id_t merged_page_id = dir_page->GetBucketPageId(merged_bucket_idx);
245     HASH_TABLE_BUCKET_TYPE *merged_bucket = FetchBucketPage(merged_page_id);
246     if (dir_page->GetLocalDepth(merged_bucket_idx) == local_depth && merged_bucket->IsEmpty()) {
247       local_depth--;
248       uint32_t mask = (1 << local_depth) - 1;
249       uint32_t idx = mask & bucket_idx;
250       uint32_t records_num = 1 << (global_depth - local_depth);
251       uint32_t step = (1 << local_depth);
252 
253       for (uint32_t i = 0; i < records_num; i++) {
254         dir_page->SetBucketPageId(idx, bucket_page_id);
255         dir_page->DecrLocalDepth(idx);
256         idx += step;
257       }
258       buffer_pool_manager_->DeletePage(merged_page_id);
259     }
260     if (dir_page->CanShrink()) {
261       dir_page->DecrGlobalDepth();
262     }
263     assert(buffer_pool_manager_->UnpinPage(merged_page_id, true, nullptr));
264   }
265   table_latch_.WUnlock();
266   assert(buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr));
267   assert(buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr));
268 }
```

在`Merge`函数获取写锁后，需要重新判断是否满足合并条件，以防止在释放锁的空隙时页面被更改，在合并被执行时，需要判断当前目录页面是否可以收缩，如可以搜索在这里仅需递减全局深度即可完成收缩，最后释放页面和写锁。