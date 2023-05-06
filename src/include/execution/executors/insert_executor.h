//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * 将元组插入到表中并更新索引
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 * 插入的值总是从子执行器中提取的。
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context执行器所需的上下文（系统资源）
   * @param plan The insert plan to be executed 通过成员 plan 来得知该如何进行本次计算
   * @param child_executor The child executor from which inserted tuples are pulled
   * @param child_executor 从中提取插入元组的子执行器
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;

  const TableInfo *table_info_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<IndexInfo *> table_indexes_;
  bool is_end_{false};
};

}  // namespace bustub
