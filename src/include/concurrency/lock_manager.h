//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.*/
class LockManager {
 public:

 //五种锁，共享锁，排他锁，意向共享锁，意向排他锁，共享意向排他锁
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   * 结构来保存锁定请求。这可以是表或行上的锁定请求。对于表锁定请求，rid_属性将不被使用。
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not 记录锁是否已授予锁定*/
    bool granted_{false};
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) 
     * 对同一资源（表或行）的锁定请求列表
    */
    std::list<std::shared_ptr<LockRequest>> request_queue_;

    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) 升级事务的ID*/
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() {
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    enable_cycle_detection_ = false;
    cycle_detection_thread_->join();
    delete cycle_detection_thread_;
  }

  /**
   * [LOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   *    Both LockTable() and LockRow() are blocking methods; they should wait till the lock is granted and then return.
   *    If the transaction was aborted in the meantime, do not grant the lock and return false.
   *    LockTable（）和LockRow（）都是阻塞方法；他们应该等到锁定被授予后再返回。
   *    如果在此期间事务中止，请不要授予锁定并返回false。
   *
   *
   * MULTIPLE TRANSACTIONS:
   *    LockManager should maintain a queue for each resource; locks should be granted to transactions in a FIFO manner.
   *    If there are multiple compatible lock requests, all should be granted at the same time
   *    as long as FIFO is honoured.
   *    LockManager应该为每个资源维护一个队列；应该以先进先出的方式将锁授予事务。
   *    如果有多个兼容的锁定请求，只要FIFO得到遵守，所有请求都应该同时被授予。
   *
   * SUPPORTED LOCK MODES:
   *    Table locking should support all lock modes.
   *    Row locking should not support Intention locks. Attempting this should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (ATTEMPTED_INTENTION_LOCK_ON_ROW)
   *    表锁定应支持所有锁定模式。行锁定不应支持Intention锁定。
   *    尝试此操作应将TransactionState设置为ABORTED，并引发TransactionAbortException（ATTEMPTED_INTENTION_LOCK_ON_ROW）
   *
   * 
   * ISOLATION LEVEL:
   *    一个事务只在必要且允许的情况下获得锁
   *    Depending on the ISOLATION LEVEL, a transaction should attempt to take locks:
   *    - Only if required, AND
   *    - Only if allowed
   * 
   *    
   *    
   *    For instance S/IS/SIX locks are not   required under READ_UNCOMMITTED, and any such attempt should set the
   *    TransactionState as ABORTED and throw a TransactionAbortException (LOCK_SHARED_ON_READ_UNCOMMITTED).
   *    在READ_UNCOMITTED下不需要S/IS/SIX锁，并且任何这样的尝试都应该设置
   *    ansactionState为ABORTED，并引发TransactionAbortException（LOCK_SHARED_ON_READ_UNCOMITTED）。
   *
   *    Similarly, X/IX locks on rows are not allowed if the the Transaction State is SHRINKING, and any such attempt
   *    should set the TransactionState as ABORTED and throw a TransactionAbortException (LOCK_ON_SHRINKING).
   *    类似地，如果事务状态为SHRINKING，则不允许对行执行X/IX锁，
   *    并且任何此类尝试都应将事务状态设置为ABORTED并引发TransactionAbortException（LOCK_on_SHRINKING）。
   *
   *    REPEATABLE_READ:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        No locks are allowed in the SHRINKING state
   *         可重复读取（_READ）：
          *该事务需要获取所有锁。
          *所有锁都允许处于GROWING状态
          *在收缩状态下不允许使用锁 
   *
   * 
   *    READ_COMMITTED:
   *        The transaction is required to take all locks.
   *        All locks are allowed in the GROWING state
   *        Only IS, S locks are allowed in the SHRINKING state
   *      *已提交读取（_co）：
          *该事务需要获取所有锁。
          *所有锁都允许处于GROWING状态
          *在收缩状态下只允许使用IS、S锁

   *
   *    READ_UNCOMMITTED:
   *        The transaction is required to take only IX, X locks.
   *        X, IX locks are allowed in the GROWING state.
   *        S, IS, SIX locks are never allowed
   *      *读取未提交（_U）：
          *该事务只需要获取IX、X个锁。
          *在GROWING状态下允许使用X、IX锁。
          *绝不允许使用S、IS、SIX锁
   *
   *
   * MULTILEVEL LOCKING:
   *    While locking rows, Lock() should ensure that the transaction has an appropriate lock on the table which the row
   *    belongs to. For instance, if an exclusive lock is attempted on a row, the transaction must hold either
   *    X, IX, or SIX on the table. If such a lock does not exist on the table, Lock() should set the TransactionState
   *    as ABORTED and throw a TransactionAbortException (TABLE_LOCK_NOT_PRESENT)
   *    多级锁定：
        锁定行时，Lock（）应确保事务对该行所属的表具有适当的锁定。
        例如，如果试图对某行进行独占锁定，则事务必须在表上保留X、IX或SIX。
        如果表上不存在这样的锁，则lock（）应将TransactionState设置为ABORTED，并引发TransactionAbortException（table_lock_not_PRESENT）
   *
   *
   * LOCK UPGRADE:
   *    Calling Lock() on a resource that is already locked should have the following behaviour:
   *    - If requested lock mode is the same as that of the lock presently held,
   *      Lock() should return true since it already has the lock.
   *    - If requested lock mode is different, Lock() should upgrade the lock held by the transaction.
   *      对已锁定的资源调用Lock（）应具有以下行为
        *-如果请求的锁定模式与当前持有的锁定模式相同，
        *Lock（）应该返回true，因为它已经有了锁。
        *-如果请求的锁模式不同，则lock（）应升级事务所持有的锁。
   *
   *    A lock request being upgraded should be prioritised over other waiting lock requests on the same resource.
   *    正在升级的锁定请求应优先于同一资源上的其他等待锁定请求。
   *
   *    While upgrading, only the following transitions should be allowed:
   *    锁升级的规则
   *        IS -> [S, X, IX, SIX]
   *        S -> [X, SIX]
   *        IX -> [X, SIX]
   *        SIX -> [X]
   *    Any other upgrade is considered incompatible, and such an attempt should set the TransactionState as ABORTED
   *    and throw a TransactionAbortException (INCOMPATIBLE_UPGRADE)
   *    其他的升级都是不被允许的，应该抛出异常
   *    
   *    Furthermore, only one transaction should be allowed to upgrade its lock on a given resource.
   *    Multiple concurrent lock upgrades on the same resource should set the TransactionState as
   *    ABORTED and throw a TransactionAbortException (UPGRADE_CONFLICT).、
   *    此外，应该只允许一个事务升级其对给定资源的锁定。
   *    同一资源上的多个并发锁升级应将TransactionState设置为ABORTED，并引发TransactionAbortException（UPGRADE_CONFLICT）。
   *
   *
   * BOOK KEEPING:
   *    If a lock is granted to a transaction, lock manager should update its
   *    lock sets appropriately (check transaction.h)
   *    如果向事务授予了锁，则锁管理器应该适当地更新其锁集（checktransaction.h）
   */

  /**
   * [UNLOCK_NOTE]
   *
   * GENERAL BEHAVIOUR:
   * 一般行为：
   * UnlockTable（）和UnlockRow（）都应该释放对资源的锁定并返回。
   * 两者都应确保事务当前对其试图解锁的资源持有锁定。
   * 如果没有，LockManager应将TransactionState设置为ABORTED并抛出
   * 事务中止异常（ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD）
   *    Both UnlockTable() and UnlockRow() should release the lock on the resource and return.
   *    Both should ensure that the transaction currently holds a lock on the resource it is attempting to unlock.
   *    If not, LockManager should set the TransactionState as ABORTED and throw
   *    a TransactionAbortException (ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD)
   *
   *    Additionally, unlocking a table should only be allowed if the transaction does not hold locks on any
   *    row on that table. If the transaction holds locks on rows of the table, Unlock should set the Transaction State
   *    as ABORTED and throw a TransactionAbortException (TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS).
   *    此外，只有当事务没有锁定表上的任何行时，才允许解锁该表。
   *    如果事务在表的行上持有锁，Unlock应将transaction State设置为ABORTED，并引发TransactionAbortException（table_UNLOCKED_BEFORE_UNLOCKING_rows）。
   *
   *    Finally, unlocking a resource should also grant any new lock requests for the resource (if possible).
   *    最后，解锁资源也应该允许对该资源的任何新的锁定请求（如果可能的话）。
   * 
    * TRANSACTION STATE UPDATE
    *    Unlock should update the transaction state appropriately (depending upon the ISOLATION LEVEL)
    *    Only unlocking S or X locks changes transaction state.
    *
    *    REPEATABLE_READ:
    *        Unlocking S/X locks should set the transaction state to SHRINKING
    *
    *    READ_COMMITTED:
    *        Unlocking X locks should set the transaction state to SHRINKING.
    *        Unlocking S locks does not affect transaction state.
    *
    *   READ_UNCOMMITTED:
    *        Unlocking X locks should set the transaction state to SHRINKING.
    *        S locks are not permitted under READ_UNCOMMITTED.
    *            The behaviour upon unlocking an S lock under this isolation level is undefined.
    * 
    * *事务状态更新
      *解锁应适当更新事务状态（取决于隔离级别）
      *只有解锁S或X锁才能更改事务状态。

      *
      *可重复读取（_READ）：
      *解锁S/X锁应将事务状态设置为收缩

      *
      *已提交读取（_co）：
      *解锁X锁应将事务状态设置为SHRINKING。
      *解锁S锁不会影响事务状态。

      *
      *读取未提交（_U）：
      *解锁X锁应将事务状态设置为SHRINKING。
      *在READ_UNCOMITTED下不允许使用S锁。
      *在此隔离级别下解锁S锁时的行为是未定义的。
   *
   *
   * BOOK KEEPING:
   *    After a resource is unlocked, lock manager should update the transaction's lock sets
   *    appropriately (check transaction.h)
   */

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  auto GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                 const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool;

  auto InsertOrDeleteTableLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert)
      -> void;

  auto InsertOrDeleteRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert)
      -> void;

  auto InsertRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      lock_set->emplace(oid, std::unordered_set<RID>{});
      row_lock_set = lock_set->find(oid);
    }
    row_lock_set->second.emplace(rid);
  }

  auto DeleteRowLockSet(const std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> &lock_set,
                        const table_oid_t &oid, const RID &rid) -> void {
    auto row_lock_set = lock_set->find(oid);
    if (row_lock_set == lock_set->end()) {
      return;
    }
    row_lock_set->second.erase(rid);
  }

  //从txn_id进行DFS检测wait-for图是否存在环
  auto Dfs(txn_id_t txn_id) -> bool {
    //txn_id已经遍历过，知道是安全的，则直接返回false
    if (safe_set_.find(txn_id) != safe_set_.end()) {
      return false;
    }
    //否则将其加入到active_set_参与后续检测
    active_set_.insert(txn_id);

    std::vector<txn_id_t> &next_node_vector = waits_for_[txn_id];
    std::sort(next_node_vector.begin(), next_node_vector.end());
    for (txn_id_t const next_node : next_node_vector) {
      if (active_set_.find(next_node) != active_set_.end()) {
        return true;
      }
      if (Dfs(next_node)) {
        return true;
      }
    }

    active_set_.erase(txn_id);
    safe_set_.insert(txn_id);
    return false;
  }

  auto DeleteNode(txn_id_t txn_id) -> void;

 private:
  /** Fall 2022 */
  /** Structure that holds lock requests for a given table oid 
   * 记录 table 和与其相关锁请求的映射关系。
  */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID 
   * 记录 row 和与其相关锁请求的映射关系。
  */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;

  std::set<txn_id_t> safe_set_;
  std::set<txn_id_t> txn_set_;
  std::unordered_set<txn_id_t> active_set_;

  std::unordered_map<txn_id_t, RID> map_txn_rid_;
  std::unordered_map<txn_id_t, table_oid_t> map_txn_oid_;
};

}  // namespace bustub
