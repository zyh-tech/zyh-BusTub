# Task 1 Access Method Executors

Task 1 包含 4 个算子，SeqScan、Insert、Delete 和 IndexScan。

## SeqScan
读取给定 table 中的所有 tuple，仅会出现在查询计划的叶子节点处。直接使用已经提供的 TableIterator逐条遍历即可。实现起来挺简单的，不过多说明。

## Insert、Delete

Insert 和 Delete 这两个算子实现起来基本一样，也比较特殊，是唯二的写算子。数据库最主要的操作就是增查删改。Bustub sql 层暂时不支持 UPDATE。Insert 和 Delete 一定是查询计划的根节点，且仅需返回一个代表修改行数的 tuple。

Insert 和 Delete 时，记得要更新与 table 相关的所有 index。index 与 table 类似，同样由 Catalog 管理。需要注意的是，由于可以对不同的字段建立 index，一个 table 可能对应多个 index，所有的 index 都需要更新。

Insert 时，直接将 tuple 追加至 table 尾部。Delete 时，并不是直接删除，而是将 tuple 标记为删除状态，也就是逻辑删除。（在事务提交后，再进行物理删除，Project 3 中无需实现）

Insert & Delete 的 Next() 只会返回一个包含一个 integer value 的 tuple，表示 table 中有多少行受到了影响。



## IndexScan

使用我们在 Project 2 中实现的 B+Tree Index Iterator，遍历 B+ 树叶子节点。由于我们实现的是非聚簇索引，在叶子节点只能获取到 RID，需要拿着 RID 去 table 查询对应的 tuple。注意处理过滤谓词。