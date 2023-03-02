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
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
  is_successful_ = false;
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (aht_iterator_ != aht_.End()) {
    // 事实上这块的 group by clause 不一定要加到 tuple 中，
    // 测试代码并不会获取该 group by clause，它只会获取下面的 value，
    // 但是它获取 value 是通过地址偏移解决的，然而如果你不加这个 group by clause，
    // 就会造成地址偏移不定。
    // 从结果推原因，测试代码 get value 获取的地址偏移是 schema 中 column 决定的，
    // 而这个 offset 是起初就已经定好了，所以必须按照这个规定来
    // std::cout << plan_->output_schema_->ToString() << std::endl;
    std::vector<Value> v(aht_iterator_.Key().group_bys_);
    for (const auto &i : aht_iterator_.Val().aggregates_) {
      v.push_back(i);
    }
    *tuple = {v, plan_->output_schema_.get()};
    // std::cout << tuple->ToString(plan_->output_schema_.get()) << std::endl;
    ++aht_iterator_;
    is_successful_ = true;
    return true;
  }
  // 没成功过，需要输出一个 null 值
  if (!is_successful_) {
    is_successful_ = true;
    // 这里要加一个 group cluase 为空的判断
    // 因为测试代码中，如果 group by 为空，则 no output
    if (plan_->group_bys_.empty()) {
      *tuple = {aht_.GenerateInitialAggregateValue().aggregates_, plan_->output_schema_.get()};
      return true;
    }
  }
  return false;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
