# Task 3 Concurrent Query Execution

我们仅需修改 SeqScan、Insert 和 Delete 三个算子。为什么其他的算子不需要修改？因为其他算子获取的 tuple 数据均为中间结果，并不是表中实际的数据。而这三个算子是需要与表中实际数据打交道的。其中 Insert 和 Delete 几乎完全一样，与 SeqScan 分别代表着写和读。

## SeqScan

如果隔离级别是 READ_UNCOMMITTED 则无需加锁。加锁失败则抛出 ExecutionException 异常。

在 READ_COMMITTED 下，在 Next() 函数中，若表中已经没有数据，则提前释放之前持有的锁。在 REPEATABLE_READ 下，在 Commit/Abort 时统一释放，无需手动释放。

实际上需要给表加 IS 锁，再给行加 S 锁。另外，在 Leaderboard Test 里，我们需要实现一个 Predicate pushdown to SeqScan 的优化，即将 Filter 算子结合进 SeqScan 里，这样我们仅需给符合 predicate 的行加上 S 锁，减小加锁数量。

那么在实现了 Predicate pushdown to SeqScan 之后，有没有可以给表直接加 S 锁的情况？有，当 SeqScan 算子中不存在 Predicate 时，即需要全表扫描时，或许可以直接给表加 S 锁，避免给所有行全部加上 S 锁。


## Insert & Delete

在 Init() 函数中，为表加上 IX 锁，再为行加 X 锁。同样，若获取失败则抛 ExecutionException 异常。另外，这里的获取失败不仅是结果返回 false，还有可能是抛出了 TransactionAbort() 异常，例如 UPGRADE_CONFLICT，需要用 try catch 捕获。

锁在 Commit/Abort 时统一释放，无需手动释放。