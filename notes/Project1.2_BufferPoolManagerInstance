## Task2 : BUFFER POOL MANAGER INSTANCE

在本任务中，需要实现缓冲池管理模块，其从`DiskManager`中获取数据库页面，并在缓冲池强制要求时或驱逐页面时将数据库脏页面写回`DiskManager`。


缓冲池的成员如下所示(实验环境给出)，其中`pages_`为缓冲池中的实际容器页面槽位数组，用于存放从磁盘中读入的页面，；`disk_manager_`为磁盘管理器，负责从磁盘读出页面和将页面写入磁盘；
·`log_manager_`为日志管理器，本实验中未使用；`page_table_`用于保存磁盘页面ID`page_id`和槽位ID`frame_id_t`的映射；`raplacer_`用于选取所需驱逐的页面；
`free_list_`保存缓冲池中的空闲槽位ID。
在这里，区分`page_id`和`frame_id_t`是完成本实验的关键。

```C++
 30 class BufferPoolManagerInstance : public BufferPoolManager {
...
134   Page *pages_;
135   /** Pointer to the disk manager. */
136   DiskManager *disk_manager_ __attribute__((__unused__));
137   /** Pointer to the log manager. */
138   LogManager *log_manager_ __attribute__((__unused__));
139   /** Page table for keeping track of buffer pool pages. */
140   std::unordered_map<page_id_t, frame_id_t> page_table_;
141   /** Replacer to find unpinned pages for replacement. */
142   Replacer *replacer_;
143   /** List of free pages. */
144   std::list<frame_id_t> free_list_;
145   /** This latch protects shared data structures. We recommend updating this comment to describe     what it protects. */
146   std::mutex latch_;
147 };

```

`Page`是缓冲池中的页面容器，`data_`保存对应磁盘页面的实际数据；`page_id_`保存该页面在磁盘管理器中的页面ID；`pin_count_`保存DBMS中正使用该页面的用户数目；
`is_dirty_`保存该页面自磁盘读入或写回后是否被修改。
## 代码实现
```C++
 28 class Page {
 29   // There is book-keeping information inside the page that should only be relevant to the buffer     pool manager.
 30   friend class BufferPoolManagerInstance;
 31 
 32  public:
 33   /** Constructor. Zeros out the page data. */
 34   Page() { ResetMemory(); }
 35 
 36   /** Default destructor. */
 37   ~Page() = default;
 38 
 39   /** @return the actual data contained within this page */
 40   inline auto GetData() -> char * { return data_; }
 41 
 42   /** @return the page id of this page */
 43   inline auto GetPageId() -> page_id_t { return page_id_; }
 44 
 45   /** @return the pin count of this page */
 46   inline auto GetPinCount() -> int { return pin_count_; }
 47 
 48   /** @return true if the page in memory has been modified from the page on disk, false otherwise     */
 49   inline auto IsDirty() -> bool { return is_dirty_; }
...
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


下面，将介绍实验中要求我们完成的五个函数：
`FlushPgImp`用于将指定缓冲池页面写回磁盘。
首先，应当检查缓冲池中是否存在对应页面ID的页面，如不存在则返回False；
如存在对应页面，则将缓冲池内的该页面的`is_dirty_`置为false，
并使用`WritePage`将该页面的实际数据`data_`写回磁盘。

```C++
 51 bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
 52   // Make sure you call DiskManager::WritePage!
 53   frame_id_t frame_id;
 54   latch_.lock();
 55   if (page_table_.count(page_id) == 0U) {
 56     latch_.unlock();
 57     return false;
 58   }
 59   frame_id = page_table_[page_id];
 60   pages_[frame_id].is_dirty_ = false;
 61   disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
 62   latch_.unlock();
 63   return true;
 64 }
```

`FlushAllPgsImp`将缓冲池内的所有页面写回磁盘。
遍历`page_table_`以获得缓冲池内所有<页面ID - 槽位ID>对，
并使用`WritePage`将每个页面的实际数据`data_`写回磁盘。
```C++
 66 void BufferPoolManagerInstance::FlushAllPgsImp() {
 67   // You can do it!
 68   latch_.lock();
 69   for (auto [page_id, frame_id] : page_table_) {
 70     pages_[frame_id].is_dirty_ = false;
 71     disk_manager_->WritePage(page_id, pages_[frame_id].GetData());
 72   }
 73   latch_.unlock();
 74 }
```


`NewPgImp`在磁盘中分配新的物理页面，将其添加至缓冲池，
并返回指向缓冲池页面`Page`的指针。在这里，该函数由以下步骤组成：
1. 检查当前缓冲池中是否存在空闲槽位或存放页面可被替换的槽位（下文称其为目标槽位），
在这里总是先通过检查`free_list_`以查询空闲槽位，如无空闲槽位则尝试从`replace_`中驱逐页面并返回被选中驱逐页面的槽位。
如目标槽位不存在，则返回空指针；如存在目标槽位，则调用`AllocatePage()`为新的物理页面分配`page_id`页面ID。
2. 在这里需要检查选中的目标槽位中的页面是否为脏页面，如是则需将其写回磁盘，并将其脏位设为false；
3. 从`page_table_`中删除目标槽位中的原页面ID的映射，并将新的<页面ID - 槽位ID>映射插入，然后更新槽位中页面的数据，
在这里由于我们返回了指向该页面的指针，我们需要将该页面的用户数`pin_count_`置为1，并调用`replacer_`的`Pin`。

```C++
 76 Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
 77   // 0.   Make sure you call AllocatePage!
 78   // 1.   If all the pages in the buffer pool are pinned, return nullptr.
 79   // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
 80   // 3.   Update P's metadata, zero out memory and add P to the page table.
 81   // 4.   Set the page ID output parameter. Return a pointer to P.
 82   frame_id_t new_frame_id;
 83   latch_.lock();
 84   if (!free_list_.empty()) {
 85     new_frame_id = free_list_.front();
 86     free_list_.pop_front();
 87   } else if (!replacer_->Victim(&new_frame_id)) {
 88     latch_.unlock();
 89     return nullptr;
 90   }
 91   *page_id = AllocatePage();
 92   if (pages_[new_frame_id].IsDirty()) {
 93     page_id_t flush_page_id = pages_[new_frame_id].page_id_;
 94     pages_[new_frame_id].is_dirty_ = false;
 95     disk_manager_->WritePage(flush_page_id, pages_[new_frame_id].GetData());
 96   }
 97   page_table_.erase(pages_[new_frame_id].page_id_);
 98   page_table_[*page_id] = new_frame_id;
 99   pages_[new_frame_id].page_id_ = *page_id;
100   pages_[new_frame_id].ResetMemory();
101   pages_[new_frame_id].pin_count_ = 1;
102   replacer_->Pin(new_frame_id);
103   latch_.unlock();
104   return &pages_[new_frame_id];
105 }
```



`FetchPgImp`的功能是获取对应页面ID的页面，并返回指向该页面的指针，其由以下步骤组成：

1. 首先，通过检查`page_table_`以检查缓冲池中是否已经缓冲该页面，如果缓冲池已经缓冲该页面，则直接返回该页面，
并将该页面的用户数`pin_count_`递增以及调用`replacer_`的`Pin`方法；
2. 如缓冲池中尚未缓冲该页面，使用disk_manager_读出该页面的内容，之后则和`NewPgImp`中的过程类似，将该页面加入缓冲池中。
唯一的区别是这次的page_id是参数传入的而不是调用AllocatePage()分配的。


```C++
107 Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
108   // 1.     Search the page table for the requested page (P).
109   // 1.1    If P exists, pin it and return it immediately.
110   // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
111   //        Note that pages are always found from the free list first.
112   // 2.     If R is dirty, write it back to the disk.
113   // 3.     Delete R from the page table and insert P.
114   // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
115   frame_id_t frame_id;
116   latch_.lock();
117   if (page_table_.count(page_id) != 0U) {
118     frame_id = page_table_[page_id];
119     pages_[frame_id].pin_count_++;
120     replacer_->Pin(frame_id);
121     latch_.unlock();
122     return &pages_[frame_id];
123   }
124 
125   if (!free_list_.empty()) {
126     frame_id = free_list_.front();
127     free_list_.pop_front();
128     page_table_[page_id] = frame_id;
129     disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
130     pages_[frame_id].pin_count_ = 1;
131     pages_[frame_id].page_id_ = page_id;
132     replacer_->Pin(frame_id);
133     latch_.unlock();
134     return &pages_[frame_id];
135   }
136   if (!replacer_->Victim(&frame_id)) {
137     latch_.unlock();
138     return nullptr;
139   }
140   if (pages_[frame_id].IsDirty()) {
141     page_id_t flush_page_id = pages_[frame_id].page_id_;
142     pages_[frame_id].is_dirty_ = false;
143     disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
144   }
145   page_table_.erase(pages_[frame_id].page_id_);
146   page_table_[page_id] = frame_id;
147   pages_[frame_id].page_id_ = page_id;
148   disk_manager_->ReadPage(page_id, pages_[frame_id].data_);
149   pages_[frame_id].pin_count_ = 1;
150   replacer_->Pin(frame_id);
151   latch_.unlock();
152   return &pages_[frame_id];
153 }
```


`DeletePgImp`的功能为从缓冲池中删除对应页面ID的页面，并将其插入空闲链表`free_list_`，
`DeletePgImp`的返回值代表的是该页面是否被用户使用，其由以下步骤组成：

1. 首先，检查该页面是否存在于缓冲区，如果不存在则返回True。
2.如果存在则检查该页面的用户数`pin_count_`是否为0，如非0则返回False。
3.检查该页面是否为脏，如是则将其写回并将脏位设置为False。
4.在`page_table_`中删除该页面的映射，
并将该槽位中页面的`page_id`置为`INVALID_PAGE_ID`。最后，将槽位ID插入空闲链表即可。

```C++
155 bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
156   // 0.   Make sure you call DeallocatePage!
157   // 1.   Search the page table for the requested page (P).
158   // 1.   If P does not exist, return true.
159   // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
160   // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
161   DeallocatePage(page_id);
162   latch_.lock();
163   if (page_table_.count(page_id) == 0U) {
164     latch_.unlock();
165     return true;
166   }
167   frame_id_t frame_id;
168   frame_id = page_table_[page_id];
169   if (pages_[frame_id].pin_count_ != 0) {
170     latch_.unlock();
171     return false;
172   }
173   if (pages_[frame_id].IsDirty()) {
174     page_id_t flush_page_id = pages_[frame_id].page_id_;
175     pages_[frame_id].is_dirty_ = false;
176     disk_manager_->WritePage(flush_page_id, pages_[frame_id].GetData());
177   }
178   page_table_.erase(page_id);
179   pages_[frame_id].page_id_ = INVALID_PAGE_ID;
180   free_list_.push_back(frame_id);
181   latch_.unlock();
182   return true;
183 }
```


`UnpinPgImp`的功能为提供用户向缓冲池通知页面使用完毕的接口，
用户需传入两个参数，使用完毕页面的页面ID以及使用过程中是否对该页面进行修改。其由以下步骤组成：

1. 首先，需检查该页面是否在缓冲池中，如未在缓冲池中则返回True。然后，检查该页面的用户数是否大于0，如不存在用户则返回false；
2. 递减该页面的用户数`pin_count_`，如在递减后该值等于0，则调用`replacer_->Unpin`以表示该页面可以被驱逐。

```C++
185 bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
186   latch_.lock();
187   frame_id_t frame_id;
188   if (page_table_.count(page_id) != 0U) {
189     frame_id = page_table_[page_id];
190     pages_[frame_id].is_dirty_ |= is_dirty;
191     if (pages_[frame_id].pin_count_ <= 0) {
192       latch_.unlock();
193       return false;
194     }
195     // std::cout<<"Unpin : pin_count = "<<pages_[frame_id].pin_count_<<std::endl;
196     if (--pages_[frame_id].pin_count_ == 0) {
197       replacer_->Unpin(frame_id);
198     }
199   }
200   latch_.unlock();
201   return true;
202 }
```
