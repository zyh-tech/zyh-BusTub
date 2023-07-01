# Leaderboard Task

Leaderboard Task 包含三条极其诡异的 sql，我们要做的就是增加新的优化规则，让这三条 sql 执行地越快越好。

## Query 1: Where's the Index?

```
create index t1x on t1_50k(x);

select count(*), max(t1_50k.x), max(t1_50k.y), max(__mock_t2_100k.x), max(__mock_t2_100k.y), max(__mock_t3_1k.x), max(__mock_t3_1k.y) from (
    t1_50k inner join __mock_t2_100k on t1_50k.x = __mock_t2_100k.x
) inner join __mock_t3_1k on __mock_t2_100k.y = __mock_t3_1k.y;
```
稍微把表名替换一下：
```
create index t1x on t1(x);

select count(*), max(t1.x), max(t1.y), max(t2.x), max(t2.y), max(t3.x), max(t3.y) from (
    t1 inner join t2 on t1.x = t2.x
) inner join t3 on t2.y = t3.y;
```

实际上就是三张表 Join 再 Aggregate 一下。主要优化方向是把 NestedLoopJoin 替换为 HashJoin、Join Reorder 让小表驱动大表，以及正确识别 t1.x 上的索引。


NestLoopJoin算法简单来说，就是双重循环，遍历外表(驱动表)，对于外表的每一行记录，然后遍历内表，然后判断join条件是否符合，进而确定是否将记录吐出给上一个执行节点。从算法角度来说，这是一个M*N的复杂度。HashJoin是针对equal-join场景的优化，基本思想是，将外表数据load到内存，并建立hash表，这样只需要遍历一遍内表，就可以完成join操作，输出匹配的记录。如果数据能全部load到内存当然好，逻辑也简单，一般称这种join为CHJ(Classic Hash Join)，之前MariaDB就已经实现了这种HashJoin算法。如果数据不能全部load到内存，就需要分批load进内存，然后分批join，下面具体介绍这几种join算法的实现。
In-Memory Join(CHJ)

HashJoin一般包括两个过程，创建hash表的build过程和探测hash表的probe过程。

1).build phase

遍历外表，以join条件为key，查询需要的列作为value创建hash表。这里涉及到一个选择外表的依据，主要是评估参与join的两个表(结果集)的大小来判断，谁小就选择谁，这样有限的内存更容易放下hash表。

2).probe phase

hash表build完成后，然后逐行遍历内表，对于内表的每个记录，对join条件计算hash值，并在hash表中查找，如果匹配，则输出，否则跳过。所有内表记录遍历完，则整个过程就结束了。



Join Reorder 其实比较简单，可以调用 EstimatedCardinality() 来估计 table 的大小，然后根据大小来调整 plan tree 里连续 join 的顺序即可。

## Query 2: Too Many Joins!
先看看 sql：

select count(*), max(__mock_t4_1m.x), max(__mock_t4_1m.y), max(__mock_t5_1m.x), max(__mock_t5_1m.y), max(__mock_t6_1m.x), max(__mock_t6_1m.y)
    from (select * from __mock_t4_1m, __mock_t5_1m where __mock_t4_1m.x = __mock_t5_1m.x), __mock_t6_1m
        where (__mock_t6_1m.y = __mock_t5_1m.y)
            and (__mock_t4_1m.y >= 1000000) and (__mock_t4_1m.y < 1500000) and (__mock_t6_1m.x < 150000) and (__mock_t6_1m.x >= 100000);

更精神恍惚了，简化一下：

select count(*), max(t4.x), max(t4.y), max(t5.x), max(t5.y), max(t6.x), max(t6.y)
    from (select * from t4, t5 where t4.x = t5.x), t6
        where (t6.y = t5.y) and (t4.y >= 1000000) and (t4.y < 1500000) and (t6.x < 150000) and (t6.x >= 100000);

这大概是个什么东西呢，大概是所有的 JOIN 全部写成了 FULL JOIN，然后把所有 Filter 放在了 plan tree 的顶端。


需要优化的内容还是比较明显的，Filter Push-down，将 Filter 尽可能地下推至数据源处。通过尽早过滤掉数据，能大大减少数据处理的量，降低资源消耗，在同样的服务器环境下，极大地减少了查询/处理时间。需要注意不是所有的 Filter 都可以下推。在本例中，我们只需要把 Filter 正确下推至 Join 算子下就可以了。


需要注意的时，我们下推的不是整个 Filter 节点，实际上是节点中的 predicate。我的做法是遍历表达式树，提取 predicate 中的所有 comparison，判断表达式的两边是否一个是 column value，一个是 const value，
（类似 A.age > 10）只有这样的 predicate 可以被下推（也存在其他形式的可以下推的 predicate，由于在这里只是对 optimizer 的体验，也只用优化预先给出的 sql，可以稍微简化一下算法，不用考虑太多的 corner case），再将所有的 predicate 重新组合为 logic expression，生成新的 Filter，根据 column value 的 idx 来选择下推的分支。

两边都为 column value 且分别代表左右两个下层算子的某一列的 Filter 可以被结合到 Join 节点中作为 Join 条件。这一步的规则已经被实现好了，因此这种 Filter 我们让其停留在原地即可。


## Query 3: The Mad Data Scientist
看看 sql：
```
select v, d1, d2 from (
    select
        v, max(v1) as d1, max(v1) + max(v1) + max(v2) as d2,
        min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2), min(v1), max(v2), min(v2), max(v1) + min(v1), max(v2) + min(v2)
    from __mock_t7 left join (select v4 from __mock_t8 where 1 == 2) on v < v4 group by v
)
```

实际上，我们只用 SELECT v, d1, d2，其余的数据都是多余的，无需计算。因此我们需要实现 Column Pruning 优化。

我实现的 Column Pruning 包括两个部分：

    *   遇到连续的两个 Projection，合并为 1 个，只取上层 Projection 所需列。
    *   遇到 Projection +  Aggregation，改写 aggregates，截取 Projection 中需要的项目，其余直接抛弃。

同样地，我个人认为 Column Pruning 也是自顶向下地改写比较方便。具体实现是收集 Projection 里的所有 column，然后改写下层节点，仅保留上层需要 project 的 column。这里的 column 不一定是表中的 column，而是一种广义的 column，例如 SELECT t1.x + t1.y FROM t1 中的 t1.x + t1.y。

另外，我们注意到这条 sql 里有一个永为假的 predicate where 1 == 2。对于这种 Filter，我们可以将其优化为 DummyScan，即第一次调用 Next() 就返回 false。可以用一个空的 Value 算子实现。