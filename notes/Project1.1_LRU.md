# Project 1 : BUFFER POOL

在本实验中，您将为BusTub DBMS构建一个新的面向磁盘的存储管理器。这样的存储管理器假定数据库的主存储位置在磁盘上。


## Task1 : LRU REPLACEMENT POLICY

第一个编程项目是在存储管理器中实现一个缓冲池。缓冲池负责将物理页面从主内存来回移动到磁盘。它允许DBMS支持大于系统可用内存量的数据库。缓冲池的操作对系统中的其他部分是透明的。例如，系统使用页面的唯一标识符（page_id_t）向缓冲池请求页面，但它不知道该页面是否已经在内存中，也不知道系统是否必须从磁盘中检索该页面。

您的实现需要是线程安全的。多个线程将同时访问内部数据结构，因此您需要确保它们的关键部分受到锁存器的保护（这些锁存器在操作系统中被称为“锁”）。

 本部分中需要实现缓冲池中的`LRUReplacer`，该组件的功能是跟踪缓冲池内的页面使用情况，并在缓冲池容量不足时驱除缓冲池中最近最少使用的页面。其应当具备如下接口：

- `Victim(frame_id_t*)`：驱逐缓冲池中最近最少使用的页面，并将其内容存储在输入参数中。当`LRUReplacer`为空时返回False，否则返回True；
- `Pin(frame_id_t)`：当缓冲池中的页面被用户访问时，该方法被调用使得该页面从`LRUReplacer`中驱逐，以使得该页面固定在缓存池中；
- `Unpin(frame_id_t)`：当缓冲池的页面被所有用户使用完毕时，该方法被调用使得该页面被添加在`LRUReplacer`，使得该页面可以被缓冲池驱逐；
- `Size()`：返回`LRUReplacer`中页面的数目；


## 代码实现

LRU策略可以由**哈希表加双向链表**的方式实现，其中链表充当队列的功能以记录页面被访问的先后顺序，哈希表则记录<页面ID - 链表节点>键值对，以在O(1)复杂度下删除链表元素。实际实现中使用STL中的哈希表`unordered_map`和双向链表`list`，并在`unordered_map`中存储指向链表节点的`list::iterator`。

在双向链表的实现中，使用一个伪头部（dummy head）和伪尾部（dummy tail）标记界限，这样在添加节点和删除节点的时候就不需要检查相邻的节点是否存在。

```c++
 class LinkListNode {
 public:
  frame_id_t val_{0};
  LinkListNode *prev_{nullptr};
  LinkListNode *next_{nullptr};
  explicit LinkListNode(frame_id_t Val) : val_(Val) {}
};

class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

  void DeleteNode(LinkListNode *curr);

 private:
  // TODO(student): implement me!
  std::unordered_map<frame_id_t, LinkListNode *> data_idx_;
  LinkListNode *head_{nullptr};
  LinkListNode *tail_{nullptr};
  std::mutex data_latch_;
};
```


对于`Victim`，首先判断链表是否为空，如不为空则返回链表首节点的页面ID，并在哈希表中解除指向首节点的映射。为了保证线程安全，整个函数应当由`mutex`互斥锁保护，下文中对互斥锁操作不再赘述。

```C++
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  data_latch_.lock();
  if (data_idx_.empty()) {
    data_latch_.unlock();
    return false;
  }
  *frame_id = head_->val_;
  data_idx_.erase(head_->val_);
  LinkListNode *tmp = head_;
  DeleteNode(tmp);
  data_latch_.unlock();
  return true;
}
```


对于`Pin`，其检查`LRUReplace`中是否存在对应页面ID的节点，如不存在则直接返回，如存在对应节点则通过哈希表中存储的迭代器删除链表节点，并解除哈希表对应页面ID的映射。

```C++
void LRUReplacer::Pin(frame_id_t frame_id) {
  data_latch_.lock();
  auto it = data_idx_.find(frame_id);
  if (it != data_idx_.end()) {
    DeleteNode(data_idx_[frame_id]);
    data_idx_.erase(it);
  }
  data_latch_.unlock();
}
```

对于`Unpin`，其检查`LRUReplace`中是否存在对应页面ID的节点，如存在则直接返回，如不存在则在链表尾部插入页面ID的节点，并在哈希表中插入<页面ID - 链表尾节点>映射。
```C++
void LRUReplacer::Unpin(frame_id_t frame_id) {
  data_latch_.lock();
  auto it = data_idx_.find(frame_id);
  if (it == data_idx_.end()) {
    LinkListNode *new_node = new LinkListNode(frame_id);
    if (data_idx_.empty()) {
      head_ = tail_ = new_node;
    } else {
      tail_->next_ = new_node;
      new_node->prev_ = tail_;
      tail_ = new_node;
    }
    data_idx_[frame_id] = tail_;
  }
  data_latch_.unlock();
}
```

对于`Size`，返回哈希表大小即可。
```C++
size_t LRUReplacer::Size() {
  data_latch_.lock();
  size_t ret = data_idx_.size();
  data_latch_.unlock();
  return ret;
}
```


