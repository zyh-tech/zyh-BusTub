# Task 4 : EXTENDIBLE HASH INDEX

在本项目中，需要实现一个**磁盘备份**的**可扩展哈希表**，用于DBMS中的索引检索。磁盘备份指该哈希表可写入至磁盘中，在系统重启时可以将其重新读取至内存中使用。可扩展哈希表是动态哈希表的一种类型，其特点为桶在充满或清空时可以桶为单位进行桶分裂或合并，尽在特定情况下进行哈希表全表的扩展和收缩，以减小扩展和收缩操作对全表的影响。

首先要了解**低位可拓展哈希表**的原理及其实现 
https://www.geeksforgeeks.org/extendible-hashing-dynamic-approach-to-dbms/


## Task 1 : PAGE LAYOUTS

为了能在磁盘中写入和读取该哈希表，需要实现两个页面类存储哈希表的数据，其使用上实验中的`Page`页面作为载体，以在磁盘中被写入和读取，具体的实现原理将在下文中介绍：

### HashTableDirectoryPage

该页面类作为哈希表的目录页面，保存哈希表中使用的所有元数据，包括该页面的页面ID，日志序列号以及哈希表的全局深度、局部深度及各目录项所指向的桶的页面ID。下面将展示一些稍有难度的函数实现：
```C++
 25 /**
 26  *
 27  * Directory Page for extendible hash table.
 28  *
 29  * Directory format (size in byte):
 30  * --------------------------------------------------------------------------------------------
 31  * | LSN (4) | PageId(4) | GlobalDepth(4) | LocalDepths(512) | BucketPageIds(2048) | Free(1524)
 32  * --------------------------------------------------------------------------------------------
 33  */
 34 class HashTableDirectoryPage {
 35  public:
 ...
189  private:
190   page_id_t page_id_;
191   lsn_t lsn_;
192   uint32_t global_depth_{0};
193   uint8_t local_depths_[DIRECTORY_ARRAY_SIZE];
194   page_id_t bucket_page_ids_[DIRECTORY_ARRAY_SIZE];
195 };

```

`GetGlobalDepthMask`通过位运算返回用于计算全局深度低位的掩码；`CanShrink()`检查当前所有有效目录项的局部深度是否均小于全局深度，以判断是否可以进行表合并。
```C++
 29 uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { return (1U << global_depth_) - 1; }
...
 47 bool HashTableDirectoryPage::CanShrink() {
 48   uint32_t bucket_num = 1 << global_depth_;
 49   for (uint32_t i = 0; i < bucket_num; i++) {
 50     if (local_depths_[i] == global_depth_) {
 51       return false;
 52     }
 53   }
 54   return true;
 55 }
```



### HashTableBucketPage
该页面类用于存放哈希桶的键值与存储值对，以及桶的槽位状态数据。`occupied_`数组用于统计桶中的槽是否被使用过，当一个槽被插入键值对时，其对应的位被置为1，事实上，`occupied_`完全可以被一个`size`参数替代，但由于测试用例中需要检测对应的`occupied`值，因此在这里仍保留该数组；`readable_`数组用于标记桶中的槽是否被占用，当被占用时该值被置为1，否则置为0；`array_`是C++中一种弹性数组的写法，在这里只需知道它用于存储实际的键值对即可。
```C++
 37 template <typename KeyType, typename ValueType, typename KeyComparator>
 38 class HashTableBucketPage {
 39  public:
...
141  private:
142   // For more on BUCKET_ARRAY_SIZE see storage/page/hash_table_page_defs.h
143   char occupied_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
144   // 0 if tombstone/brand new (never occupied), 1 otherwise.
145   char readable_[(BUCKET_ARRAY_SIZE - 1) / 8 + 1];
146   // Do not add any members below array_, as they will overlap.
147   MappingType array_[0];
```



在这里，使用`char`类型存放两个状态数据数组，在实际使用应当按位提取对应的状态位。下面是使用位运算的状态数组读取和设置函数：

```C++
 87 template <typename KeyType, typename ValueType, typename KeyComparator>
 88 void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
 89   readable_[bucket_idx / 8] &= ~(1 << (7 - (bucket_idx % 8)));
 90 }
 91 
 92 template <typename KeyType, typename ValueType, typename KeyComparator>
 93 bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
 94   return (occupied_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
 95 }
 96 
 97 template <typename KeyType, typename ValueType, typename KeyComparator>
 98 void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
 99   occupied_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
100 }
101 
102 template <typename KeyType, typename ValueType, typename KeyComparator>
103 bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
104   return (readable_[bucket_idx / 8] & (1 << (7 - (bucket_idx % 8)))) != 0;
105 }
106 
107 template <typename KeyType, typename ValueType, typename KeyComparator>
108 void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
109   readable_[bucket_idx / 8] |= 1 << (7 - (bucket_idx % 8));
110 }
```

对于对应索引的键值读取直接访问`arrat_`数组即可：

```C++
 77 template <typename KeyType, typename ValueType, typename KeyComparator>
 78 KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
 79   return array_[bucket_idx].first;
 80 }
 81 
 82 template <typename KeyType, typename ValueType, typename KeyComparator>
 83 ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
 84   return array_[bucket_idx].second;
 85 }
```


`GetValue`提取桶中键为`key`的所有值，实现方法为遍历所有`occupied_`为1的位，并将键匹配的值插入`result`数组即可，如至少找到了一个对应值，则返回真。
```C++
 22 template <typename KeyType, typename ValueType, typename KeyComparator>
 23 bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
 24   bool ret = false;
 25   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 26     if (!IsOccupied(bucket_idx)) {
 27       break;
 28     }
 29     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0) {
 30       result->push_back(array_[bucket_idx].second);
 31       ret = true;
 32     }
 33   }
 34   return ret;
 35 }

```


`Insert`向桶插入键值对，其先检测该键值对是否已经被插入到桶中，如是则返回假；如未找到该键值对，则从小到大遍历所有`occupied_`为1的位，如出现`readable_`为1的位，则在`array_`中对应的数组中插入键值对。
```C++
 37 template <typename KeyType, typename ValueType, typename KeyComparator>
 38 bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
 39   size_t slot_idx = 0;
 40   bool slot_found = false;
 41   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 42     if (!slot_found && (!IsReadable(bucket_idx) || !IsOccupied(bucket_idx))) {
 43       slot_found = true;
 44       slot_idx = bucket_idx;
 45       // LOG_DEBUG("slot_idx = %ld", bucket_idx);
 46     }
 47     if (!IsOccupied(bucket_idx)) {
 48       break;
 49     }
 50     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx    )) {
 51       return false;
 52     }
 53   }
 54   if (slot_found) {
 55     SetReadable(slot_idx);
 56     SetOccupied(slot_idx);
 57     array_[slot_idx] = MappingType(key, value);
 58     return true;
 59   }
 60   return false;
 61 }
```


`Remove`从桶中删除对应的键值对，遍历桶所有位，找到目标后调用RemoveAt即可。
```C++
 63 template <typename KeyType, typename ValueType, typename KeyComparator>
 64 bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
 65   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
 66     if (!IsOccupied(bucket_idx)) {
 67       break;
 68     }
 69     if (IsReadable(bucket_idx) && cmp(key, KeyAt(bucket_idx)) == 0 && value == ValueAt(bucket_idx    )) {
 70       RemoveAt(bucket_idx);
 71       return true;
 72     }
 73   }
 74   return false;
 75 }
```


`NumReadable()`返回桶中的键值对个数，遍历统计即可。
`IsFull()`和`IsEmpty()`直接调用`NumReadable()`实现。
```C++
112 template <typename KeyType, typename ValueType, typename KeyComparator>
113 bool HASH_TABLE_BUCKET_TYPE::IsFull() {
114   return NumReadable() == BUCKET_ARRAY_SIZE;
115 }
116 
117 template <typename KeyType, typename ValueType, typename KeyComparator>
118 uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
119   uint32_t ret = 0;
120   for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
121     if (!IsOccupied(bucket_idx)) {
122       break;
123     }
124     if (IsReadable(bucket_idx)) {
125       ret++;
126     }
127   }
128   return ret;
129 } 
130     
131 template <typename KeyType, typename ValueType, typename KeyComparator>
132 bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
133   return NumReadable() == 0;
134 }
```


### Page与上述两个页面类的转换

在本部分中，有难点且比较巧妙的地方在于理解上述两个页面类是如何与`Page`类型转换的。在这里，上述两个页面类并非未`Page`类的子类，在实际应用中通过`reinterpret_cast`将`Page`与两个页面类进行转换。在这里我们回顾一下`Page`的数据成员：

```C++
 77  private:
 78   /** Zeroes out the data that is held within the page. */
 79   inline void ResetMemory() { memset(data_, OFFSET_PAGE_START, PAGE_SIZE); }
 80 
 81   /** The actual data that is stored within a page. */
 82   char data_[PAGE_SIZE]{};
 83   /** The ID of this page. */
 84   page_id_t page_id_ = INVALID_PAGE_ID;
 85   /** The pin count of this page. */
 86   int pin_count_ = 0;
 87   /** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
 88   bool is_dirty_ = false;
 89   /** Page latch. */
 90   ReaderWriterLatch rwlatch_;
 91 };
```
可以看出，`Page`中用于存放实际数据的`data_`数组位于数据成员的第一位，其在栈区固定分配一个页面的大小。因此，在`Page`与两个页面类强制转换时，通过两个页面类的指针的操作仅能影响到`data_`中的实际数据，而影响不到其它元数据。并且在内存管理器中始终是进行所占空间更大的通用页面`Page`的分配（实验中的`NewPage`），因此页面的容量总是足够的。