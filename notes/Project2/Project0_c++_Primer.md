# Project 2 : B+ TREE INDEX

Project 2 需要实现 B+ 树索引。拆分为两个部分：

  *  Checkpoint1: 单线程 B+ 树

  *  Checkpoint2: 多线程 B+ 树

实验中给出的 B+ 树接口非常简单，基本只有查询、插入和删除三个接口，内部基本没有给出别的辅助函数，可以让我们自由发挥。因此，任何合法的 B+ 树实现都是允许的。

B+ 树索引在 Bustub 中的位置如图所示：
![alt 属性文本](https://pic1.zhimg.com/v2-9bbaeeb4929774b00fa209d4ad0026cc_r.jpg)
需要使用我们在 Project 1 中实现的 buffer pool manager 来获取 page。





# Checkpoint1 Single Thread B+Tree

Checkpoint1 分为两个部分：

*  Task1: B+Tree pages，B+树中的各种 page。在 Bustub 索引 B+ 树中，所有的节点都是 page。包含 leaf page，internal page ，和它们的父类 tree page。
    
*  Task2：B+Tree Data Structure (Insertion, Deletion, Point Search)。Checkpoint1 的重点，即 B+树的插入、删除和单点查询。



## Task1 B+Tree Pages

### page
```
/** The actual data that is stored within a page. */
char data_[BUSTUB_PAGE_SIZE]{};
/** The ID of this page. */
page_id_t page_id_ = INVALID_PAGE_ID;
/** The pin count of this page. */
int pin_count_ = 0;
/** True if the page is dirty, i.e. it is different from its corresponding page on disk. */
bool is_dirty_ = false;
/** Page latch. */
ReaderWriterLatch rwlatch_;
```

其中，data_ 是实际存放 page 数据的地方，大小为 BUSTUB_PAGE_SIZE，为 4KB。其他的成员是 page 的 metadata。B+树中的 tree oage 数据均存放在 page 的 data 成员中。

### B_PLUS_TREE_PAGE
b_plus_tree_page 是另外两个 page 的父类，即 B+树中 tree page 的抽象。


```
IndexPageType page_type_;   // leaf or internal. 4 Byte
lsn_t lsn_  // temporarily unused. 4 Byte
int size_;  // tree page data size(not in byte, in count). 4 Byte
int max_size_;  // tree page data max size(not in byte, in count). 4 Byte
page_id_t parent_page_id_; // 4 Byte
page_id_t page_id_; // 4 Byte
// 24 Byte in total
```
以上数据组成了 tree page 的 header。


page data 的 4KB 中，24Byte 用于存放 header，剩下的则用于存放 tree page 的数据，即 KV 对。



### B_PLUS_TREE_INTERNAL_PAGE

对应 B+ 树中的内部节点。

MappingType array_[1];

internal page 中没有新的 metadata，header 大小仍为 24B。它唯一的成员是这个怪怪的大小为 1 的数组。大小为 1 显然不合理，代表只能存放一个 KV 对。但又没法改变它的大小，难道要用 undefined behavior 来越界访问其后的地址？实际上差不多就是这个意思。但这不是 undefined behavior，是一种特殊的写法，叫做 flexible array。我也不知道怎么翻译。

简单来说就是，当你有一个类，这个类中有一个成员为数组。在用这个类初始化一个对象时，你不能确定该将这个数组的大小设置为多少，但知道这整个对象的大小是多少 byte，你就可以用到 flexible array。flexible array 必须是类中的最后一个成员，并且仅能有一个。在为对象分配内存时，flexible array 会自动填充，占用未被其他变量使用的内存。这样就可以确定自己的长度了。

实际上这就是 C++ 对象内存布局的一个简单的例子。因此 flexible array 为什么只能有一个且必须放在最后一个就很明显了，因为需要向后尝试填充。

此外，虽然成员在内存中的先后顺序和声明的顺序一致，但需要注意可能存在的内存对齐的问题。header 中的数据大小都为 4 byte，没有对齐问题。

到这里，这个大小为 1 的数组的作用就比较清楚了。利用 flexible array 的特性来自动填充 page data 4KB 减掉 header 24byte 后剩余的内存。剩下的这些内存用来存放 KV 对。


因此page的布局如图所示：
![alt 属性文本](https://pic1.zhimg.com/80/v2-47d960f0d651057d0b64f259c79488d8_1440w.webp)




internal page 中，KV 对的 K 是能够比较大小的索引，V 是 page id，用来指向下一层的节点。Project 中要求，第一个 Key 为空。主要是因为在 internal page 中，n 个 key 可以将数轴划分为 n+1 个区域，也就对应着 n+1 个 value。实际上你也可以把最后一个 key 当作是空的，只要后续的处理自洽就可以了。

![alt 属性文本](https://pic2.zhimg.com/80/v2-979e77ae892d1bda6609ac71a0282529_1440w.webp)
通过比较 key 的大小选中下一层的节点。需要注意的是，internal page 中的 key 并不代表实际上的索引值，仅仅是作为一个向导，引导需要插入/删除/查询的 key 找到这个 key 真正所在的 leaf page。



### B_PLUS_TREE_LEAF_PAGE
leaf page 和 internal page 的内存布局基本一样，只是 leaf page 多了一个成员变量 next_page_id，指向下一个 leaf page（用于 range scan）。因此 leaf page 的 header 大小为 28 Byte。

leaf page 的 KV 对中，K 是实际的索引，V 是 record id。record id 用于识别表中的某一条数据。leaf page 的 KV 对是一一对应的，不像 internal page 的 value 多一个。这里也可以看出来 Bustub 所有的 B+ 树索引，无论是主键索引还是二级索引都是非聚簇索引。


## Task2 B+Tree Data Structure (Insertion, Deletion, Point Search)

Task2 是单线程 B+ 树的重点。首先提供演示一个 B+ 树插入删除操作的网站https://goneill.co.nz/btree-demo.php。主要是看看 B+ 树插入删除的各种细节变化。当然具体实现是自由的，这仅仅是一个示例。

### Search
先从最简单的 Point Search 开始。B+ 树的结构应该都比较熟悉了，节点分为 internal page 和 leaf page，每个 page 上的 key 有序排列。当拿到一个 key 需要查找对应的 value 时，首先需要经由 internal page 递归地向下查找，最终找到 key 所在的 leaf page。这个过程可以简化为一个函数 Findleaf()。

Findleaf() 从 root page 开始查找。在查找到 leaf page 时直接返回，否则根据 key 在当前 internal page 中找到对应的 child page id，递归调用 Findleaf。根据 key 查找对应 child id 时，由于 key 是有序的，可以直接进行二分搜索。


internal page 中储存 key 和 child page id，那么在拿到 page id 后如何获得对应的 page 指针？用 Project 1 中实现的 buffer pool。

```
Page *page = buffer_pool_manager_->FetchPage(page_id);
```

同样地，假如我们需要新建一个 page，也是调用 buffer pool 的 NewPage()。

在获取到一个 page 后，如何使用这个 page 来存储数据？之前已经提到过，page 的 data_ 字段是实际用于存储数据的 4KB 大小的字节数组。通过 reinterpret_cast 将这个字节数组强制转换为我们要使用的类型，例如 leaf page：
```
auto leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData())
```

查找的过程比较简单。但还有一个比较重要且复杂的细节，就是 page unpin 的问题。

我们在拿到 page id 后，调用 buffer pool 的 FetchPage() 函数来获取对应的 page 指针。要注意的是，在使用完 page 之后，需要将 page unpin 掉，否则最终会导致 buffer pool 中的所有 page 都被 pin 住，无法从 disk 读取其他的 page。

比较合适的做法是，在本次操作中，找出 page 最后一次被使用的地方，并在最后一次使用后 unpin()。


### Insert
与 Search 相同，第一步是根据 key 找到需要插入的 leaf page。同样是调用 Findleaf()。得到 leaf page 后，将 key 插入 leaf page。要注意的是，插入时仍需保证 key 的有序性。同样可以二分搜索找到合适的位置插入。

在插入后，需要检查当前 leaf page size 是否等于 max size。若相等，则要进行一次 leaf page 分裂操作。具体步骤为：

    * 1.新建一个空的 page，
    * 2.将原 page 的一半转移到新 page 中，（假如选择将新 page 放在原 page 右侧，则转移原 page 的右半部分）'
    * 3.更新原 page 和新 page 的 next page id，
    * 4.获取 parent page，
    * 5.将用于区分原 page 和新 page 的 key 插入 parent page 中，
    * 6.更新 parent page 所有 child page 的父节点指针。
这些步骤都比较好理解。需要给 parent page 插入一个新 key 的原因是，多了一个子节点，自然需要多一个 key 来区分。其中第 4 步是重点。获取 parent page 并不是简单地通过当前 page 的 parent id 来获取，因为 parent page 也可能发生分裂。

在第 5 步我们可以拿到 parent page 安全地插入 KV 对，是因为在第 4 步中，我们需要返回一个安全的 parent page。

第 4 步具体操作如下： 
* 1.根据 parent page id 拿到 parent page， 
* 2.判断 parent page size 是否等于 max size，（插入前检查） 
* 3.若小于，直接返回 parent page， 否则，分裂当前 internal page。并根据此后需要插入的 key 选择分裂后的两个 page 之一作为 parent page 返回。

分裂 internal page 的步骤为： 
* 1.新建一个空的 page，
* 2.将原 page 的一半转移到新 page 中，需要注意原 page 和新 page 的第一个 key 都是无效的， 
* 3.更新新 page 所有 child page 的父节点指针，指向新 page， 
* 4.获取 parent page， 
* 5.将用于区分原 page 和新 page 的 key 插入 parent page 中， 
* 6.更新 parent page 所有 child page 的父节点指针。

可以发现，第 4 步同样是需要重复上述步骤。这里就发生了向上的递归，直到遇到安全的父节点或者遇到根节点。在遇到根节点时，若根节点也需要分裂，则除了需要新建一个节点用来容纳原根节点一半的 KV 对，还需要新建一个新的根节点。

另外需要注意一个细节，在 leaf page 分裂时，向父节点插入 key 时是复制后插入，而 internal page 分裂时，向父节点插入 key 是删除后插入，有点像把 key 上推。


### Delete
同样地，先找到 leaf page。删除 leaf page 中 key 对应的 KV 对后，检查 size 是否小于 min size。如果小于的话，首先尝试从两侧的兄弟节点中偷一个 KV 对。注意只能从兄弟节点，即父节点相同的节点中选取。假如存在一侧节点有富余的 KV 对，则成功偷取，结束操作。若两侧都没有富余的 KV 对，则选择一侧节点与其合并。

偷取的过程比较简单，从左侧节点偷取时，把左侧节点最后一个 KV 对转移至当前节点第一个 KV 对，从右侧节点偷取时，把右侧节点的 KV 对转移至当前节点最后一个 KV 对。leaf page 和 internal page 的偷取过程基本相同，仅需注意 internal page 偷取后更新子节点的父节点指针。

稍难的是合并的过程。同样，任选左右侧一兄弟节点进行合并。将一个节点的所有 KV 对转移至另一节点。若合并的是 leaf page，记得更新 next page id。若合并的是 internal page，记得更新合并后 page 的子节点的父节点指针。然后，删除 parent 节点中对应的 key。删除后，再次检查 size 是否小于 min size，形成向上递归。

当合并 leaf page 后，删除父节点中对应的 key 比较简单，直接删除即可。

需要注意的是，root page 并不受 min size 的限制。但如果 root page 被删到 size 只剩 1，即只有一个 child page 的时候，应将此 child page 设置为新的 root page。

另外，在合并时，两个 page 合并成一个 page，另一个 page 应该删除，释放资源。删除 page 时，仍是调用 buffer pool 的 DeletePage() 函数。

和 Insert 类似，Delete 过程也是先向下递归查询 leaf page，不满足 min size 后先尝试偷取，无法偷取则合并，并向上递归地检查是否满足 min size。


# Checkpoint2 Multi Thread B+Tree

Checkpoint2 也分为两个部分： 
* 1.Task3：Index Iterator。实现 leaf page 的 range scan。 
* 2.Task4：Concurrent Index。支持 B+ 树并发操作。

## Task3 Index Iterator
这个部分没有什么太多好说的，实现一个遍历 leaf page 的迭代器。在迭代器中存储当前 leaf page 的指针和当前停留的位置即可。遍历完当前 page 后，通过 next page id 找到下一个 leaf page。同样，记得 unpin 已经遍历完的 page。

## Task4 Concurrent Index
这是并发 B+ 树的重点，应该也是 Project2 中最难的部分。我们要使此前实现的 B+ 树支持并发的 Search/Insert/Delete 操作。整棵树一把锁逻辑上来说当然是可以的，但性能也会可想而知地糟糕。在这里，我们会使用一种特殊的加锁方式，叫做 latch crabbing。其基本思想是： 
* 1.先锁住 parent page， 
* 2.再锁住 child page， 
* 3.假设 child page 是安全的，则释放 parent page 的锁。安全指当前 page 在当前操作下一定不会发生 split/steal/merge。同时，安全对不同操作的定义是不同的，Search 时，任何节点都安全；Insert 时，判断 max size；Delete 时，判断 min size。

这么做的原因和正确性还是比较明显的。当 page 为安全的时候，当前操作仅可能改变此 page 及其 child page 的值，因此可以提前释放掉其祖先的锁来提高并发性能。

### Search
Search 时，从 root page 开始，先给 parent 上读锁，再给 child page 上读锁，然后释放 parent page 的锁。如此向下递归。

###  Insert
Insert 时，从 root page 开始，先给 parent 上写锁，再给 child page 上写锁。假如 child page 安全，则释放所有祖先的锁；否则不释放锁，继续向下递归。

在 child page 不安全时，需要持续持有祖先的写锁。并在出现安全的 child page 后，释放所有祖先写锁。如何记录哪些 page 当前持有锁？这里就要用到在 Checkpoint1 里一直没有提到的一个参数，transaction。

transaction 就是 Bustub 里的事务。在 Project2 中，可以暂时不用理解事务是什么，而是将其看作当前在对 B+ 树进行操作的线程。调用 transaction 的 AddIntoPageSet() 方法，来跟踪当前线程获取的 page 锁。在发现一个安全的 child page 后，将 transaction 中记录的 page 锁全部释放掉。按理来说，释放锁的顺序可以从上到下也可以从下到上，但由于上层节点的竞争一般更加激烈，所以最好是从上到下地释放锁。

在完成整个 Insert 操作后，释放所有锁。


Delete

和 Insert 基本一样。仅是判断是否安全的方法不同（检测 min size）。需要另外注意的是，当需要 steal/merge sibling 时，也需要对 sibling 加锁。并在完成 steal/merge 后马上释放。这里是为了避免其他线程正在对 sibling 进行 Search/Insert 操作，从而发生 data race。这里的加锁就不需要在 transaction 里记录了，只是临时使用。