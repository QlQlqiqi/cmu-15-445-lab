//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "type/integer_parent_type.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
  SetLSN(INVALID_LSN);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndexOf(const KeyType &key, const KeyComparator &keyComparator) -> int {
  for (int i = GetSize() - 1; i >= 0; i--) {
    if (keyComparator(KeyAt(i), key) == 0) {
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) {
  array_[GetSize()] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) {
  for (int i = GetSize() - 1; i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  array_[0] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitL(const KeyComparator &keyComparator, BufferPoolManager *buffer_pool_manager_)
    -> Page * {
  // new page
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page_ptr = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page_ptr == nullptr) {
    throw Exception("Insert failed: can not get a new Page\n");
  }
  new_page_ptr->WLatch();
  auto new_node_ptr = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page_ptr->GetData());
  new_node_ptr->Init(new_page_id, GetParentPageId(), GetMaxSize());
  // 将 (min_size, size) 移到新的 page
  int split_index = GetMinSize();
  for (int i = split_index, sz = GetSize(); i < sz; i++) {
    new_node_ptr->InsertLast(KeyAt(i), ValueAt(i));
  }
  new_node_ptr->SetNextPageId(GetNextPageId());
  SetNextPageId(new_page_id);
  // buffer_pool_manager_->UnpinPage(new_page_id, true);
  SetSize(split_index);
  return new_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertL(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator)
    -> bool {
  // duplicate key
  if (IsDuplicateKeyL(key, keyComparator)) {
    return false;
  }
  int i = GetSize();
  for (; i > 0; i--) {
    if (keyComparator(KeyAt(i - 1), key) < 0) {
      break;
    }
    if (keyComparator(KeyAt(i - 1), key) == 0) {
      return false;
    }
    array_[i] = array_[i - 1];
  }
  array_[i] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &keyComparator) {
  int idx = KeyIndexOf(key, keyComparator);
  if (idx == INVALID_PAGE_ID) {
    return;
  }
  for (int i = idx, sz = GetSize() - 1; i < sz; i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsDuplicateKeyL(const KeyType &key, const KeyComparator &keyComparator) -> bool {
  for (int i = 0, sz = GetSize(); i < sz; i++) {
    if (keyComparator(KeyAt(i), key) == 0) {
      return true;
    }
  }
  return false;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
