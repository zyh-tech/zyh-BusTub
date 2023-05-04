## Task 3 ：PARALLEL BUFFER POOL MANAGER

任务1.2中实现的缓冲池实现的问题在于锁的粒度过大，其在进行任何一项操作时都将整个缓冲池锁住，因此几乎不存在并行性，严重影响高并发情况下的性能。
并行缓冲池的思想是分配多个独立的缓冲池，并将不同的页面ID映射至各自的缓冲池中，减少整体缓冲池的锁粒度，以增加并行性。

```C++
 25 class ParallelBufferPoolManager : public BufferPoolManager {
...
 93  private:
 94   std::vector<BufferPoolManager *> instances_;
 95   size_t start_idx_{0};
 96   size_t pool_size_;
 97   size_t num_instances_;
 98 };
```

并行缓冲池的成员如上，`instances_`存储多个独立的缓冲池，`pool_size_`记录各缓冲池的容量，`num_instances_`为独立缓冲池的个数，`start_idx`见下文。

```C++
 18 ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, Disk    Manager *disk_manager,
 19                                                      LogManager *log_manager)
 20     : pool_size_(pool_size), num_instances_(num_instances) {
 21   // Allocate and create individual BufferPoolManagerInstances
 22   for (size_t i = 0; i < num_instances; i++) {
 23     BufferPoolManager *tmp = new BufferPoolManagerInstance(pool_size, num_instances, i, disk_mana    ger, log_manager);
 24     instances_.push_back(tmp);
 25   }
 26 }
 27 
 28 // Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated me    mory
 29 ParallelBufferPoolManager::~ParallelBufferPoolManager() {
 30   for (size_t i = 0; i < num_instances_; i++) {
 31     delete (instances_[i]);
 32   }
 33 }
```
构造函数和析构函数。


```C++
 35 size_t ParallelBufferPoolManager::GetPoolSize() {
 36   // Get size of all BufferPoolManagerInstances
 37   return num_instances_ * pool_size_;
 38 }
 39 
 40 BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
 41   // Get BufferPoolManager responsible for handling given page id. You can use this method in you    r other methods.
 42   return instances_[page_id % num_instances_];
 43 }
```
`GetPoolSize`应返回全部缓冲池的容量，即独立缓冲池个数乘以缓冲池容量。

`GetBufferPoolManager`返回页面ID所对应的独立缓冲池指针，通过对页面ID取余的方式将页面ID映射至对应的缓冲池。

```C++
 45 Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
 46   // Fetch page for page_id from responsible BufferPoolManagerInstance
 47   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 48   return instance->FetchPage(page_id);
 49 }
 50 
 51 bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
 52   // Unpin page_id from responsible BufferPoolManagerInstance
 53   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 54   return instance->UnpinPage(page_id, is_dirty);
 55 }
 56 
 57 bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
 58   // Flush page_id from responsible BufferPoolManagerInstance
 59   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 60   return instance->FlushPage(page_id);
 61 }
...
 82 bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
 83   // Delete page_id from responsible BufferPoolManagerInstance
 84   BufferPoolManager *instance = GetBufferPoolManager(page_id);
 85   return instance->DeletePage(page_id);
 86 }
 87 
 88 void ParallelBufferPoolManager::FlushAllPgsImp() {
 89   // flush all pages from all BufferPoolManagerInstances
 90   for (size_t i = 0; i < num_instances_; i++) {
 91     instances_[i]->FlushAllPages();
 92   }
 93 }
```
上述函数仅需调用对应独立缓冲池的方法即可。


```C++
 63 Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
 64   // create new page. We will request page allocation in a round robin manner from the underlying
 65   // BufferPoolManagerInstances
 66   // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return     2) looped around to
 67   // starting index and return nullptr
 68   // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
 69   // is called
 70   Page *ret;
 71   for (size_t i = 0; i < num_instances_; i++) {
 72     size_t idx = (start_idx_ + i) % num_instances_;
 73     if ((ret = instances_[idx]->NewPage(page_id)) != nullptr) {
 74       start_idx_ = (*page_id + 1) % num_instances_;
 75       return ret;
 76     }
 77   }
 78   start_idx_++;
 79   return nullptr;
 80 }
```

在这里，为了使得各独立缓冲池的负载均衡，采用轮转方法选取分配物理页面时使用的缓冲池，在这里具体的规则如下：

1. 从`start_idx_`开始遍历各独立缓冲池，如存在调用`NewPage`成功的页面，则返回该页面并将`start_idx`指向该页面的下一个页面；
2. 如全部缓冲池调用`NewPage`均失败，则返回空指针，并递增`start_idx`。