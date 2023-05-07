#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_{std::move(child_executor)} {}

void SortExecutor::Init() {
  //Sort 也是 pipeline breaker。在 Init() 中读取所有下层算子的 tuple，并按 ORDER BY 的字段升序或降序排序。
  child_->Init();
  Tuple child_tuple{};
  RID child_rid;
  while (child_->Next(&child_tuple, &child_rid)) {
    child_tuples_.push_back(child_tuple);
  }

  //自定义排序规则，直接传入一个 lambda 匿名函数。由于要访问成员 plan_ 来获取排序的字段，
  //lambda 需要捕获 this 指针。另外，排序字段可以有多个，按先后顺序比较。第一个不相等，直接得到结果；相等，则比较第二个。不会出现所有字段全部相等的情况。
  std::sort(
      child_tuples_.begin(), child_tuples_.end(),
      [order_bys = plan_->order_bys_, schema = child_->GetOutputSchema()](const Tuple &tuple_a, const Tuple &tuple_b) {
        for (const auto &order_key : order_bys) {
          switch (order_key.first) {
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
            case OrderByType::ASC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
            case OrderByType::DESC:
              if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                        .CompareGreaterThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return true;
              } else if (static_cast<bool>(order_key.second->Evaluate(&tuple_a, schema)
                                               .CompareLessThan(order_key.second->Evaluate(&tuple_b, schema)))) {
                return false;
              }
              break;
          }
        }
        return false;
      });

  child_iter_ = child_tuples_.begin();
}

//next直接将容器中的tuple迭代取出即可
auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (child_iter_ == child_tuples_.end()) {
    return false;
  }

  *tuple = *child_iter_;
  *rid = tuple->GetRid();
  ++child_iter_;

  return true;
}

}  // namespace bustub
