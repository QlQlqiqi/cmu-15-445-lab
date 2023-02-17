/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page_ptr, const int &index, BufferPoolManager *bufferPoolManager)
    : page_ptr_(page_ptr), buffer_pool_manager_(bufferPoolManager), index_(index) {
  if (page_ptr == nullptr) {
    page_id_ = INVALID_PAGE_ID;
    index_ = INVALID_PAGE_ID;
    return;
  }
  page_id_ = page_ptr->GetPageId();
  auto cur_leaf_node_ptr = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr_->GetData());
  pair_ = std::make_pair(cur_leaf_node_ptr->KeyAt(index_), cur_leaf_node_ptr->ValueAt(index_));
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return pair_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // 已经结束了
  if (page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  auto cur_leaf_node_ptr = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_ptr_->GetData());
  // 需要获取下一个 page
  // ===========================
  // 这里应该尝试获取 lock，失败则 restart 或 throw exception，
  // 但是该 lab 并没有测试这个点
  // ===========================
  if (index_ == cur_leaf_node_ptr->GetSize() - 1) {
    auto next_leaf_page_id = cur_leaf_node_ptr->GetNextPageId();
    // 当前是最后一个节点
    if (next_leaf_page_id == INVALID_PAGE_ID) {
      page_ptr_->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_id_, false);
      page_id_ = INVALID_PAGE_ID;
      page_ptr_ = nullptr;
      index_ = INVALID_PAGE_ID;
      return *this;
    }

    auto next_page_ptr = buffer_pool_manager_->FetchPage(next_leaf_page_id);
    auto next_leaf_node_ptr = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page_ptr->GetData());
    next_page_ptr->RLatch();
    page_ptr_->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id_, false);
    index_ = 0;
    page_id_ = next_page_ptr->GetPageId();
    page_ptr_ = next_page_ptr;
    pair_ = std::make_pair(next_leaf_node_ptr->KeyAt(index_), next_leaf_node_ptr->ValueAt(index_));
    return *this;
  }
  index_++;
  pair_ = std::make_pair(cur_leaf_node_ptr->KeyAt(index_), cur_leaf_node_ptr->ValueAt(index_));
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
