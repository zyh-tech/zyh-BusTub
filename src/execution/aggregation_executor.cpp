//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->aggregates_, plan_->agg_types_),
      aht_iterator_(aht_.Begin()) {}

//在 Aggregation 的 Init() 函数中，我们就要将所有结果全部计算出来
void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple{};
  RID rid{};
  while (child_->Next(&tuple, &rid)) {
    //在下层算子传来一个 tuple 时，将 tuple 的 group by 字段和 aggregate 字段分别提取出来，
    //调用 InsertCombine() 将 group by 和 aggregate 的映射关系存入 SimpleAggregationHashTable。
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  if (aht_.Size() == 0 && GetOutputSchema().GetColumnCount() == 1) {
    aht_.InsertIntialCombine();
  }
  //将迭代器aht_iterator_初始化为哈希表的begin
  aht_iterator_ = aht_.Begin();
}

//在 Next() 中直接利用 hashmap iterator 将结果依次取出。

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values;
  //Aggregation 输出的 schema 形式为 group-bys + aggregates。
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
