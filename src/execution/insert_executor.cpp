//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "common/exception.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), done_(false) {}

void InsertExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ptr_ = table_info_->table_.get();
  child_executor_->Init();
  // table 加 IX
  if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                              table_info_->oid_)) {
    throw Exception("lock IX into table failed\n");
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (done_) {
    return false;
  }
  int rows = 0;
  Tuple t;
  RID r;
  while (child_executor_->Next(&t, &r)) {
    if (!table_heap_ptr_->InsertTuple(t, &r, exec_ctx_->GetTransaction())) {
      throw Exception("insert tuple failed\n");
    }
    // row lock x
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                              table_info_->oid_, r)) {
      throw Exception("lock row X failed\n");
    }
    // 更新 index
    auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    for (auto index_info : indexes) {
      auto index_key = t.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, r, exec_ctx_->GetTransaction());
    }
    rows++;
  }
  Value value(TypeId::INTEGER, rows);
  std::vector<Value> v;
  v.emplace_back(value);
  std::vector<Column> col;
  col.emplace_back("", INTEGER);
  Schema schema(col);
  Tuple tmp_tuple(v, &schema);
  *tuple = tmp_tuple;
  done_ = true;
  return true;
}
}  // namespace bustub
