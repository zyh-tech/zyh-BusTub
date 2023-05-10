# Task 1 Lock Manager

大部分需要注意的内容都在 lock_manager.h 的 LOCK NOTE 和 UNLOCK NOTE 里了。
仔细阅读后按照要求实现并不困难，但是细节较多

##  Lock Manager 的结构

    table_lock_map_：记录 table 和与其相关锁请求的映射关系。
    row_lock_map_：记录 row 和与其相关锁请求的映射关系。

这两个 map 的值均为锁请求队列 LockRequestQueue：

    request_queue_：实际存放锁请求的队列。
    cv_ & latch_：条件变量和锁，配合使用可以实现经典的等待资源的模型。
    upgrading_：正在此资源上尝试锁升级的事务 id。

锁请求以 LockRequest 类表示：

    txn_id_：发起此请求的事务 id。
    lock_mode_：请求锁的类型。
    oid_：在 table 粒度锁请求中，代表 table id。在 row 粒度锁请求中，表示 row 属于的 table 的 id。rid_：仅在 row 粒度锁请求中有效。指 row 对应的 rid。
    granted_：是否已经对此请求授予锁？

Lock Manager 的作用是处理事务发送的锁请求，例如有一个 SeqScan 算子需要扫描某张表，其所在事务就需要对这张表加 S 锁。而加读锁这个动作需要由 Lock Manager 来完成。事务先对向 Lock Manager 发起加 S 锁请求，Lock Manager 对请求进行处理。如果发现此时没有其他的锁与这个请求冲突，则授予其 S 锁并返回。如果存在冲突，例如其他事务持有这张表的 X 锁，则 Lock Manager 会阻塞此请求（即阻塞此事务），直到能够授予 S 锁，再授予并返回。


## Lock过程
以 table lock 为例。

### 第一步，检查 txn 的状态。

若 txn 处于 Abort/Commit 状态，抛逻辑异常，不应该有这种情况出现。
若 txn 处于 Shrinking 状态，则需要检查 txn 的隔离级别和当前锁请求类型：

    REPEATABLE_READ:
    The transaction is required to take all locks.
    All locks are allowed in the GROWING state
    No locks are allowed in the SHRINKING state

    READ_COMMITTED:
    The transaction is required to take all locks.
    All locks are allowed in the GROWING state
    Only IS, S locks are allowed in the SHRINKING state

    READ_UNCOMMITTED:
    The transaction is required to take only IX, X locks.
    X, IX locks are allowed in the GROWING state.
    S, IS, SIX locks are never allowed

在 Project 4 中仅需支持除了 SERIALIZABLE 外的剩下三种隔离级别。

根据txn处于的状态和请求的锁类型判断：
若 txn 处于 Shrinking 状态：

    在 REPEATABLE_READ 下，造成事务终止，并抛出 LOCK_ON_SHRINKING 异常。
    在 READ_COMMITTED 下，若为 IS/S 锁，则正常通过，否则抛 LOCK_ON_SHRINKING。
    在 READ_UNCOMMITTED 下，若为 IX/X 锁，抛 LOCK_ON_SHRINKING，否则抛 LOCK_SHARED_ON_READ_UNCOMMITTED。

若 txn 处于 Growing 状态，

    若隔离级别为 READ_UNCOMMITTED 且锁类型为 S/IS/SIX，抛 LOCK_SHARED_ON_READ_UNCOMMITTED。其余状态正常通过。

第一步保证了锁请求、事务状态、事务隔离级别的兼容。正常通过第一步后，可以开始尝试获取锁。


### 第二步，获取 table 对应的 lock request queue
从 table_lock_map_ 中获取 table 对应的 lock request queue。注意需要对 map 加锁，并且为了提高并发性，在获取到 queue 之后立即释放 map 的锁。若 queue 不存在则创建。

### 第三步，检查此锁请求是否为一次锁升级。

granted 和 waiting 的锁请求均放在同一个队列里，我们需要遍历队列查看有没有与当前事务 id（我习惯叫做 tid）相同的请求。如果存在这样的请求，则代表当前事务在此前已经得到了在此资源上的一把锁，接下来可能需要锁升级。需要注意的是，这个请求的 granted_ 一定为 true。因为假如事务此前的请求还没有被通过，事务会被阻塞在 LockManager 中，不可能再去尝试获取另一把锁。

找到了此前已经获取的锁，开始尝试锁升级。首先，判断此前授予锁类型是否与当前请求锁类型相同。若相同，则代表是一次重复的请求，直接返回。否则进行下一步检查。

接下来，判断当前资源上是否有另一个事务正在尝试升级（queue->upgrading_ == INVALID_TXN_ID）。若有，则终止当前事务，抛出 UPGRADE_CONFLICT 异常。因为不允许多个事务在同一资源上同时尝试锁升级。

然后，判断升级锁的类型和之前锁是否兼容，不能反向升级。

    While upgrading, only the following transitions should be allowed: 
    IS -> [S, X, IX, SIX] S -> [X, SIX] IX -> [X, SIX] SIX -> [X]

锁升级的步骤大概就是这样。当然，假如遍历队列后发现不存在与当前 tid 相同的请求，就代表这是一次平凡的锁请求。

### 第四步，将锁请求加入请求队列。
创建一个LockRequest，加入队列尾部,这里采用一条队列，把 granted 和 waiting 的请求放在一起，个人感觉不是特别清晰。或许可以改成一条 granted 队列和一条 waiting 队列。

### 第五步，尝试获取锁。
在 GrantLock() 中，Lock Manager 会判断是否可以满足当前锁请求。若可以满足，则返回 true，事务成功获取锁，并退出循环。若不能满足，则返回 false，事务暂时无法获取锁，在 wait 处阻塞，等待资源状态变化时被唤醒并再次判断是否能够获取锁。资源状态变化指的是什么？其他事务释放了锁。

接下来是 GrantLock() 函数。在此函数中，我们需要判断当前锁请求是否能被满足。
判断兼容性。遍历请求队列，查看当前锁请求是否与所有的已经 granted 的请求兼容。

两项检查通过后，代表当前请求既兼容又有最高优先级，因此可以授予锁。授予锁的方式是将 granted_ 置为 true。并返回 true。假如这是一次升级请求，则代表升级完成，还要记得将 upgrading_ 置为 INVALID_TXN_ID。

另外，需要进行一些 Bookkeeping 操作。Transaction 中需要维护许多集合，分别记录了 Transaction 当前持有的各种类型的锁。方便在事务提交或终止后全部释放。

Lock 的流程大致如此，row lock 与 table lock 几乎相同，仅多了一个检查步骤。在接收到 row lock 请求后，需要检查是否持有 row 对应的 table lock。必须先持有 table lock 再持有 row lock。



## UnLock过程

仍以 table lock 为例。Unlock 的流程比 Lock 要简单不少。

首先，由于是 table lock，在释放时需要先检查其下的所有 row lock 是否已经释放。

接下来是 table lock 和 row lock 的公共步骤：

第一步，获取对应的 lock request queue。

第二步，遍历请求队列，找到 unlock 对应的 granted 请求。

若不存在对应的请求，抛 ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD 异常。

找到对应的请求后，根据事务的隔离级别和锁类型修改其状态。

当隔离级别为 REPEATABLE_READ 时，S/X 锁释放会使事务进入 Shrinking 状态。当为 READ_COMMITTED 时，只有 X 锁释放使事务进入 Shrinking 状态。当为 READ_UNCOMMITTED 时，X 锁释放使事务 Shrinking，S 锁不会出现。

之后，在请求队列中 remove unlock 对应的请求，并将请求 delete。

同样，需要进行 Bookkeeping。

在锁成功释放后，调用 cv_.notify_all() 唤醒所有阻塞在此 table 上的事务，检查能够获取锁。

Task 1 的内容就是这样，核心是条件变量阻塞模型，此外细枝末节还是挺多的，需要细心维护。