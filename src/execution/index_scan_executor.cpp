//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  index_ptr_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info->index_.get());
  index_iterator_ = std::make_unique<BPlusTreeIndexIteratorForOneIntegerColumn>(index_ptr_->GetBeginIterator());
  table_heap_ptr_ = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_)->table_.get();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (*index_iterator_ == index_ptr_->GetEndIterator()) {
    return false;
  }
  *rid = (**index_iterator_).second;
  table_heap_ptr_->GetTuple(*rid, tuple, exec_ctx_->GetTransaction());
  ++(*index_iterator_);
  // index_iterator_->operator++();
  return true;
}

}  // namespace bustub
