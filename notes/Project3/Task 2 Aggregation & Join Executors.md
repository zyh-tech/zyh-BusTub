# Task 2 Aggregation & Join Executors

Task 2 包含了 3 个算子，Aggregation、NestedLoopJoin 和 NestedIndexJoin。

## Aggregation
AggregationExecutor中有一个关键的成员，SimpleAggregationHashTable。Aggregation 是 pipeline breaker。也就是说，Aggregation 算子会打破 iteration model 的规则。在 Aggregation 的 Init() 函数中，我们就要将所有结果全部计算出来。原因很简单，比如下面这条 sql：

```
SELECT t.x, max(t.y) FROM t GROUP BY t.x;
```

结果的每条 tuple 都是一个 t.x 的聚合，而要得到同一个 t.x 对应的 max(t.y)，必须要遍历整张表。因此，Aggregation 需要在 Init() 中直接计算出全部结果，将结果暂存，再在 Next() 中一条一条地 emit。而 SimpleAggregationHashTable 就是计算并保存 Aggregation 结果的数据结构。

SimpleAggregationHashTable 维护一张 hashmap，键为 AggregateKey，值为 AggregateValue，均为 std::vector<Value>。key 代表 group by 的字段的数组，value 则是需要 aggregate 的字段的数组。在下层算子传来一个 tuple 时，将 tuple 的 group by 字段和 aggregate 字段分别提取出来，调用 InsertCombine() 将 group by 和 aggregate 的映射关系存入 SimpleAggregationHashTable。若当前 hashmap 中没有 group by 的记录，则创建初值；若已有记录，则按 aggregate 规则逐一更新所有的 aggregate 字段，例如取 max/min，求 sum 等等。

在 Init() 中计算出整张 hashmap 后，在 Next() 中直接利用 hashmap iterator 将结果依次取出。Aggregation 输出的 schema 形式为 group-bys + aggregates。

## NestedLoopJoin
NestedLoopJoin 算法本身并不难，但比较容易掉进坑里。这里记录一下踩的坑。

NestedLoopJoin的伪代码大概是这样：
```python
for outer_tuple in outer_table
    for inner_tuple in inner_table
        if inner_tuple matched outer_tuple
            emit
```

有了这个例子，很容易把代码写成：
```c++
while (left_child->Next(&left_tuple)){
    while (right_child->Next(&right_tuple)){
        if (left_tuple matches right_tuple){
            *tuple = ...;   // assemble left & right together
            return true;
        }
    }
}
return false;
```

一开始看起来似乎没什么问题。然而很快可以发现有一个严重的错误，right child 在 left child 的第一次循环中就被消耗完了，之后只会返回 false。解决方法很简单，在 Init() 里先把 right child 里的所有 tuple 取出来暂存在一个数组里就好，之后直接访问这个数组。

```c++
while (left_child->Next(&left_tuple)){
    for (auto right_tuple : right_tuples){
        if (left_tuple matches right_tuple){
            *tuple = ...;   // assemble left & right together
            return true;
        }
    }
}
return false;
```

看起来好像又没什么问题。然而，同一列是可能存在 duplicate value 的。在上层算子每次调用 NestedLoopJoin 的 Next() 时，NestedLoopJoin 都会向下层算子请求新的 left tuple。但有可能上一个 left tuple 还没有和 right child 中所有能匹配的 tuple 匹配完（只匹配了第一个）。

解决方法也很简单，在算子里暂存 left tuple，每次调用 Next() 时，先用暂存的 left tuple 尝试匹配。并且要记录上一次右表匹配到的位置，不要每次都直接从右表第一行开始匹配了。右表遍历完还没有匹配结果，再去找左表要下一个 tuple。

说来说去，实际上就是注意迭代器要保存上下文信息。


## NestedIndexJoin

在进行 equi-join 时，如果发现 JOIN ON 右边的字段上建了 index，则 Optimizer 会将 NestedLoopJoin 优化为 NestedIndexJoin。具体实现和 NestedLoopJoin 差不多，只是在尝试匹配右表 tuple 时，会拿 join key 去 B+Tree Index 里进行查询。如果查询到结果，就拿着查到的 RID 去右表获取 tuple 然后装配成结果输出。