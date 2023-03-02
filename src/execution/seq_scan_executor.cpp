//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ptr_ = table_info_->table_.get();
  table_iterator_ptr_ = std::make_unique<TableIterator>(table_heap_ptr_->Begin(exec_ctx_->GetTransaction()));
  is_read_uncommitted_ = exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED;
  // table 加 IS/IX
  if (is_read_uncommitted_) {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                                table_info_->oid_)) {
      throw Exception("lock IX into table failed\n");
    }
  } else {
    if (!exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED,
                                                table_info_->oid_)) {
      throw Exception("lock IS into table failed\n");
    }
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果有锁，则 unlock
  if (!exec_ctx_->GetTransaction()->GetSharedRowLockSet()->empty() ||
      !exec_ctx_->GetTransaction()->GetExclusiveRowLockSet()->empty()) {
    // unlock row
    if (!exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), table_info_->oid_, last_rid_)) {
      throw Exception("unlock row failed\n");
    }
  }
  if (*table_iterator_ptr_ == table_heap_ptr_->End()) {
    // 释放 table IS/IX
    if (!exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), table_info_->oid_)) {
      throw Exception("unlock table failed\n");
    }
    return false;
  }
  // lock row S/X
  if (is_read_uncommitted_) {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE,
                                              table_info_->oid_, tuple->GetRid())) {
      throw Exception("lock row X failed\n");
    }
  } else {
    if (!exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED,
                                              table_info_->oid_, tuple->GetRid())) {
      throw Exception("lock row S failed\n");
    }
  }
  *tuple = *((*table_iterator_ptr_)++);
  *rid = tuple->GetRid();
  last_rid_ = *rid;
  return true;
}

}  // namespace bustub
