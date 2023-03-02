//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetParentPageId(parent_id);
  SetPageId(page_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetLSN(INVALID_LSN);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveIndex(int index) {
  IncreaseSize(-1);
  for (int i = index, sz = GetSize(); i < sz; i++) {
    array_[i] = array_[i + 1];
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindChildIndex(const ValueType &value) -> int {
  for (int i = 0, sz = GetSize(); i < sz; i++) {
    if (ValueAt(i) == value) {
      return i;
    }
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindChild(const KeyType &key, const KeyComparator &keyComparator) -> ValueType {
  for (int i = GetSize() - 1; i > 0; i--) {
    if (keyComparator(key, KeyAt(i)) >= 0) {
      return ValueAt(i);
    }
  }
  return ValueAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertLast(const KeyType &key, const ValueType &value) {
  array_[GetSize()] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertFirst(const KeyType &key, const ValueType &value) {
  for (int i = GetSize() - 1; i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  array_[0] = std::make_pair(key, value);
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitL(const KeyComparator &keyComparator, BufferPoolManager *buffer_pool_manager_)
    -> Page * {
  // new page
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page_ptr = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page_ptr == nullptr) {
    throw Exception("Insert failed: can not get a new Page\n");
  }
  new_page_ptr->WLatch();
  auto new_node_ptr = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(new_page_ptr->GetData());
  new_node_ptr->Init(new_page_id, GetParentPageId(), GetMaxSize());

  // 将 [min_size, size) 移到新的 page
  int split_index = GetMinSize();
  // auto page_id = new_node_ptr->GetPageId();
  for (int i = split_index, sz = GetSize(); i < sz; i++) {
    new_node_ptr->InsertLast(KeyAt(i), ValueAt(i));
  }
  SetSize(split_index);
  // 更新 children 的 parent id
  for (int i = 0, sz = new_node_ptr->GetSize(); i < sz; i++) {
    auto tmp_page_ptr = buffer_pool_manager_->FetchPage(ValueAt(i));
    tmp_page_ptr->WLatch();
    auto tmp_node_ptr = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(tmp_page_ptr->GetData());
    tmp_node_ptr->SetParentPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(ValueAt(i), true);
    tmp_page_ptr->WUnlatch();
  }
  return new_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertL(const KeyType &key, const ValueType &value,
                                             const KeyComparator &keyComparator) {
  int i = GetSize();
  for (; i > 1; i--) {
    if (keyComparator(KeyAt(i - 1), key) < 0) {
      break;
    }
    array_[i] = array_[i - 1];
  }
  array_[std::max(1, i)] = std::make_pair(key, value);
  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
