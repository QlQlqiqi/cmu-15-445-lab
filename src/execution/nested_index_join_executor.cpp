//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include <vector>
#include "common/exception.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  is_inner_ = plan->GetJoinType() == JoinType::INNER;
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  inner_index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  inner_table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;
  auto left_schema = child_executor_->GetOutputSchema();
  auto right_schema = inner_table_info_->schema_;
  while (child_executor_->Next(&left_tuple, &left_rid)) {
    // outer table 一定有 inner table index 的 schema，
    // 因为他们通过 index 联立的
    auto key_schema = inner_index_info_->index_->GetKeySchema();
    std::vector<Value> values;
    auto value = plan_->KeyPredicate()->Evaluate(&left_tuple, child_executor_->GetOutputSchema());
    values.push_back(value);
    Tuple key(values, key_schema);
    std::vector<RID> results;
    inner_index_info_->index_->ScanKey(key, &results, exec_ctx_->GetTransaction());
    if (results.empty()) {
      if (is_inner_) {
        continue;
      }
      // 输出一个空的
      values.clear();
      for (int i = 0, sz = left_schema.GetColumnCount(); i < sz; i++) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (int i = 0, sz = right_schema.GetColumnCount(); i < sz; i++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
      }
      *tuple = {values, &(plan_->OutputSchema())};
      return true;
    }
    values.clear();
    // 读取符合条件的 tuple
    for (auto &rid : results) {
      Tuple right_tuple;
      if (!inner_table_info_->table_->GetTuple(rid, &right_tuple, exec_ctx_->GetTransaction())) {
        throw Exception("can not find tuple with rid\n");
      }
      for (int i = 0, sz = left_schema.GetColumnCount(); i < sz; i++) {
        values.emplace_back(left_tuple.GetValue(&left_schema, i));
      }
      for (int i = 0, sz = right_schema.GetColumnCount(); i < sz; i++) {
        values.emplace_back(right_tuple.GetValue(&right_schema, i));
      }
      *tuple = {values, &(plan_->OutputSchema())};
      return true;
    }
  }
  return false;
}

}  // namespace bustub
