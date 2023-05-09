# Project4 Concurrency Control

Project 4 是 15-445 2022Fall 的最后一个部分了，在这里我们将为 Bustub 实现关系数据库中极其重要的 transaction 概念。因为课程里面提到的一些相关知识之前没有学过，特在次做一个小的记录

## Serializable

![alt 属性文本](https://pic4.zhimg.com/80/v2-37f6d888bec323bb4bf4341eefbae62b_1440w.webp)

假设我们有 T1、T2 两个事务。如上是这两个事务顺序执行的情况，称为 serial schedule。

当事务并发时，假如事务并发执行的顺序能够等于这些事务按某种顺序顺序执行的结果，那么称这些事务的并发是 serializable schedule，称这两个 schedule 是 Serializable Schedule。也就是说是正确的。事务并发执行的顺序很难确保说是等价于那种顺序执行，只能依照不同的并发策略而定。

![alt 属性文本](https://pic2.zhimg.com/80/v2-d02c39f037ee5113fd5a427f2a97e0cd_1440w.webp)

如果按照上图的情况，最终这个 Schedule 不能等价于某个顺序执行的结果，称这个 schedule 是 Non-serializable Shedule，是不正确的。因为他违反了 Isolation，T1 读取到了 T2 修改过后的值，T2 也读取到了 T1 修改过后的值。

因此，只要能够让事务执行的 Schedule 最终等价于某个 Serial Schedule，我们才能够说他是 Serializable 的，是正确的。

## Two Phase Locking（2PL）

通过并发策略就能够管理事务，是他们自动保证一个 schedule 是 serializable。不难想到，保证 schedule 正确性的方法就是合理的加锁 (locks) 策略，2PL 就是其中之一。

注意：2PL 保证正确性的方式是通过获得锁的顺序实现的，也就说我们没办法强制要求事务按照某个 serial schedule 并发，只能保证最终结果是正确的。

锁的类型及其对比

![alt 属性文本](https://pic3.zhimg.com/80/v2-0e47145c3608e765e6ceb03ef8430c2a_1440w.webp)

2PL 是一种并发控制协议，它帮助数据库在运行过程中决定某个事务是否可以访问某条数据，并且 2PL 的正常工作并不需要提前知道所有事务的执行内容，仅仅依靠已知的信息即可。2PL 能够确保最后的结果是满足某个 serial schedule 的。

2PL，顾名思义，有两个阶段：growing 和 shrinking：

    *  在 growing 阶段中，事务可以按需获取某条数据的锁，lock manager 决定同意或者拒绝。一旦开始释放锁就进了 shrinking 阶段 
    *  在 shrinking 阶段中，事务只能释放之前获取的锁，不能获得新锁，即一旦开始释放锁，之后就只能释放锁。

假如在 shrinking 阶段请求锁，那么事务很有可能会发生 conflict operations，因此为了防止这些情况的发生，2PL 强制使用两阶段，以保证 schedule 是 serializable，通过 2PL 产生的 schedule 中，各个 txn 之间的依赖关系能构成有向无环图。


![alt 属性文本](https://pic3.zhimg.com/80/v2-927e312ad9dbc846e862530e40ae1c26_1440w.webp)

但是 2PL 一个问题在于：假如一个事务 T1 在 shrinking 阶段释放锁后，这个锁被其他事务 T2 所获取，这个时候 T1 还是未提交的，但是 T2 能够读取到 T1 修改的数据。最终 T1 终止了，T2 读取到的数据就不复存在了，也就是脏读。

那么这个时候所有读到 T1 修改过的数据的事务都需要 abort，由于会递归终止，因此也称为 级联中止 (cascading aborts)。


## DeadLock

2PL 和 强2PL 都无法解决死锁的问题，但是当死锁发生时，不解决就会出现无休止的等待。解决死锁一般有两种方法：

    *  Detection：事后检测
    *  Prevention：事前阻止

### Detection
一个检测方法就是使用 wait-for 图，每个等待的事务指向持有锁的事务，如果最后 wiat-for 图有一个环，就说明检测出了一个死锁，需要通过 rollback 的方法把某个事务回滚，直到没有死锁为止。

一种实现方法是开一个后台线程，每次定期扫描所有等待队列，然后构建出一个 wait-for 图。或者另一个比较极端的方法就是，如果一个事务等待了很久还没有获得锁，就直接回滚他，SimpleDB使用的就是这种方法，不过很容易造成殃及无辜。

这里有两个设计决定：

    检测死锁的频率
    如何选择合适的 "受害者"

检测死锁的频率越高，陷入死锁的事务等待的时间越短，但消耗的 cpu 也就越多。所以这是个典型的 trade-off，通常有一个调优的参数供用户配置。

选择 "受害者" 的指标可能有很多：事务持续时间、事务的进度、事务锁住的数据数量、级联事务的数量、事务曾经重启的次数等等。在选择完 "受害者" 后，DBMS 还有一个设计决定需要做：完全回滚还是回滚到足够消除环形依赖即可。


###  Prevention

死锁预防有两种方法：

    Old Waits for Young：wait-die：假如 T2 持有锁
        T1 比 T2 老，T1 等待T2 比 T1 老，T1 终止


    Young Waits for Old：wound-wait：假如 T2持有锁
        T1 比 T2 老，抢占你的锁，T2 终止T1 比 T2 年轻，T1 等待


    Why do these schemes guarantee no deadlocks?

无论是 Old Waits for Young 还是 Young Waits for Old，只要保证 prevention 的方向是一致的，就能阻止死锁发生，其原理类似哲学家就餐设定顺序的解决方案：先给哲学家排个序，遇到获取刀叉冲突时，顺序高的优先。

    When a txn restarts, what is its (new) priority?

原来的优先级或者时间戳，防止饥饿