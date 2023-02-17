#include <unistd.h>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // UpdateRootPageId(true);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  if (IsEmpty()) {
    return false;
  }
  // fetch page
  auto cur_page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page_ptr == nullptr) {
    return false;
  }
  cur_page_ptr->RLatch();
  // std::cout << "====================================" << std::endl;
  // std::cout << "GetValue start: k: " << key << std::endl;
  // page 上锁
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(cur_page_ptr);
  }
  auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
  while (!cur_node_ptr->IsLeafPage()) {
    // fetch page
    auto next_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->FindChild(key, comparator_));
    if (next_page_ptr == nullptr) {
      return false;
    }
    // page 上锁
    next_page_ptr->RLatch();
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(next_page_ptr);
    }
    auto next_node_ptr = reinterpret_cast<InternalPage *>(next_page_ptr->GetData());
    // 释放锁
    cur_page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_ptr->GetPageId(), false);
    if (transaction != nullptr) {
      transaction->GetPageSet()->pop_front();
    }
    cur_node_ptr = next_node_ptr;
    cur_page_ptr = next_page_ptr;
  }
  auto leaf_page_ptr = cur_page_ptr;
  auto leaf_node_ptr = reinterpret_cast<LeafPage *>(leaf_page_ptr->GetData());
  auto index = leaf_node_ptr->KeyIndexOf(key, comparator_);
  if (index == INVALID_PAGE_ID) {
    leaf_page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
    if (transaction != nullptr) {
      transaction->GetPageSet()->pop_front();
    }
    return false;
  }
  result->emplace_back(leaf_node_ptr->ValueAt(index));
  leaf_page_ptr->RUnlatch();
  // buffer_pool_manager_->UnpinPage(leaf_node_ptr->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), false);
  if (transaction != nullptr) {
    transaction->GetPageSet()->pop_front();
  }
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  // std::scoped_lock<std::mutex> lock(latch_);
  auto leaf_page_ptr = FindLeafPage(key, transaction, INSERT);
  while (leaf_page_ptr == nullptr) {
    // 可能是 root page 不存在
    latch_.lock();
    if (IsEmpty()) {
      auto page_id = INVALID_PAGE_ID;
      auto page_ptr = buffer_pool_manager_->NewPage(&page_id);
      // 申请 page 失败
      if (page_id == INVALID_PAGE_ID) {
        throw Exception("Insert failed: can not get a new Page\n");
      }
      auto node_ptr = reinterpret_cast<LeafPage *>(page_ptr->GetData());
      node_ptr->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);
      buffer_pool_manager_->UnpinPage(page_id, true);
      root_page_id_ = page_id;
      // 改变 root page id
      UpdateRootPageId(true);
    }
    latch_.unlock();
    leaf_page_ptr = FindLeafPage(key, transaction, INSERT);
  }
  // std::cout << "====================================" << std::endl;
  // std::cout << "Insert start: k: " << key << " | v: " << value << std::endl;
  // insert before split
  auto leaf_node_ptr = reinterpret_cast<LeafPage *>(leaf_page_ptr->GetData());
  bool not_duplicate = leaf_node_ptr->InsertL(key, value, comparator_);
  // duplicate key
  if (!not_duplicate) {
    if (transaction == nullptr) {
      UnpinFromBottomToRoot(leaf_page_ptr->GetPageId(), buffer_pool_manager_, INSERT);
    } else {
      UnpinAndUnlock(transaction, INSERT);
    }
    return false;
  }
  auto cur_page_ptr = leaf_page_ptr;
  while (true) {
    auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
    // no split
    if (cur_node_ptr->GetSize() <= cur_node_ptr->GetMaxSize()) {
      break;
    }
    // 如果当前节点是 root node，则创建一个新的 root page
    if (cur_node_ptr->IsRootPage()) {
      page_id_t new_page_id = INVALID_PAGE_ID;
      auto new_page_ptr = buffer_pool_manager_->NewPage(&new_page_id);
      if (new_page_ptr == nullptr) {
        throw Exception("new page failed\n");
      }
      if (transaction != nullptr) {
        new_page_ptr->WLatch();
        transaction->GetPageSet()->emplace_back(new_page_ptr);
      }
      auto new_node_ptr = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
      // new_node_ptr->Init(new_page_id);
      if (cur_node_ptr->IsLeafPage()) {
        auto tmp_new_node_ptr = reinterpret_cast<LeafPage *>(new_node_ptr);
        auto tmp_cur_node_ptr = reinterpret_cast<LeafPage *>(cur_node_ptr);
        tmp_new_node_ptr->Init(new_page_id, root_page_id_, leaf_max_size_);
        for (int i = 0, sz = cur_node_ptr->GetSize(); i < sz; i++) {
          tmp_new_node_ptr->InsertLast(tmp_cur_node_ptr->KeyAt(i), tmp_cur_node_ptr->ValueAt(i));
        }
        cur_node_ptr->SetSize(0);
        cur_node_ptr->InsertFirst(tmp_new_node_ptr->KeyAt(0), tmp_new_node_ptr->GetPageId());
      } else {
        auto tmp_new_node_ptr = reinterpret_cast<InternalPage *>(new_node_ptr);
        auto tmp_cur_node_ptr = reinterpret_cast<InternalPage *>(cur_node_ptr);
        tmp_new_node_ptr->Init(new_page_id, root_page_id_, internal_max_size_);
        for (int i = 0, sz = cur_node_ptr->GetSize(); i < sz; i++) {
          tmp_new_node_ptr->InsertLast(tmp_cur_node_ptr->KeyAt(i), tmp_cur_node_ptr->ValueAt(i));
        }
        cur_node_ptr->SetSize(0);
        cur_node_ptr->InsertFirst(tmp_new_node_ptr->KeyAt(0), tmp_new_node_ptr->GetPageId());
      }
      cur_node_ptr->SetPageType(IndexPageType::INTERNAL_PAGE);
      cur_node_ptr->SetMaxSize(internal_max_size_);
      cur_page_ptr = new_page_ptr;
      cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
    }
    // InternalPage* new_node_ptr = nullptr;
    Page *new_page_ptr = nullptr;
    KeyType new_key;
    // 选择合适的 split
    if (cur_node_ptr->IsLeafPage()) {
      auto tmp_leaf_node_ptr = reinterpret_cast<LeafPage *>(cur_node_ptr);
      new_page_ptr = tmp_leaf_node_ptr->SplitL(comparator_, buffer_pool_manager_);
      auto new_node_ptr = reinterpret_cast<LeafPage *>(new_page_ptr->GetData());
      new_key = new_node_ptr->KeyAt(0);
    } else {
      new_page_ptr = cur_node_ptr->SplitL(comparator_, buffer_pool_manager_);
      auto new_node_ptr = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
      new_key = new_node_ptr->KeyAt(0);
    }
    if (transaction != nullptr) {
      // auto tmp_page_ptr = transaction->GetPageSet()->back();
      transaction->GetPageSet()->pop_back();
      cur_page_ptr->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(cur_node_ptr->GetPageId(), true);
    Page *parent_page_ptr = nullptr;
    if (transaction != nullptr) {
      parent_page_ptr = transaction->GetPageSet()->back();
    } else {
      parent_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->GetParentPageId());
      buffer_pool_manager_->UnpinPage(cur_node_ptr->GetParentPageId(), false);
    }
    new_page_ptr->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_page_ptr->GetPageId(), true);
    auto parent_node_ptr = reinterpret_cast<InternalPage *>(parent_page_ptr->GetData());
    parent_node_ptr->InsertL(new_key, new_page_ptr->GetPageId(), comparator_);
    cur_page_ptr = parent_page_ptr;
  }
  if (transaction != nullptr && transaction->GetPageSet()->empty()) {
    throw Exception("impossible");
  }
  // Print(buffer_pool_manager_);
  // std::cout << std::endl;
  if (transaction == nullptr) {
    UnpinFromBottomToRoot(cur_page_ptr->GetPageId(), buffer_pool_manager_, INSERT);
  } else {
    UnpinAndUnlock(transaction, INSERT);
  }
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinFromBottomToRoot(page_id_t page_id, BufferPoolManager *buffer_pool_manager_, Operation op) {
  while (page_id != INVALID_PAGE_ID) {
    auto node_ptr = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    buffer_pool_manager_->UnpinPage(page_id, false);
    if (op == READ) {
      buffer_pool_manager_->UnpinPage(page_id, false);
    } else {
      buffer_pool_manager_->UnpinPage(page_id, true);
    }
    page_id = node_ptr->GetParentPageId();
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, Transaction *transaction, Operation op) -> Page * {
  if (IsEmpty()) {
    return nullptr;
  }
  // fetch page
  auto cur_page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page_ptr == nullptr) {
    return nullptr;
  }
  // page 上锁
  if (transaction != nullptr) {
    if (op == READ) {
      cur_page_ptr->RLatch();
    } else {
      cur_page_ptr->WLatch();
    }
    transaction->AddIntoPageSet(cur_page_ptr);
  }
  auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
  while (!cur_node_ptr->IsLeafPage()) {
    // fetch page
    auto next_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->FindChild(key, comparator_));
    if (next_page_ptr == nullptr) {
      return nullptr;
    }
    // page 上锁
    if (transaction != nullptr) {
      transaction->AddIntoPageSet(next_page_ptr);
      if (op == READ) {
        next_page_ptr->RLatch();
      } else {
        next_page_ptr->WLatch();
      }
    }
    auto next_node_ptr = reinterpret_cast<InternalPage *>(next_page_ptr->GetData());
    // 如果 transaction 存在且 next node 是 safe 的，则释放相应的父节点
    if (transaction != nullptr && IsSafe(next_node_ptr, op)) {
      auto page_deque = *transaction->GetPageSet();
      for (auto page_ptr : page_deque) {
        if (page_ptr->GetPageId() == next_page_ptr->GetPageId()) {
          break;
        }
        if (op == READ) {
          page_ptr->RUnlatch();
        } else {
          page_ptr->WUnlatch();
        }
        buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
        transaction->GetPageSet()->pop_front();
      }
    }
    cur_node_ptr = next_node_ptr;
    cur_page_ptr = next_page_ptr;
  }
  return cur_page_ptr;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnpinAndUnlock(Transaction *transaction, Operation op) {
  if (transaction == nullptr) {
    return;
  }
  for (auto page_ptr : *transaction->GetPageSet()) {
    if (op == READ) {
      page_ptr->RUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    } else {
      page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
    }
  }
  transaction->GetPageSet()->clear();
  for (auto page_ptr : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_ptr);
  }
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsSafe(const InternalPage *node_ptr, Operation op) -> bool {
  if (node_ptr == nullptr) {
    return false;
  }
  if ((op == REMOVE && node_ptr->GetSize() <= node_ptr->GetMinSize()) ||
      (op == INSERT && node_ptr->GetSize() >= node_ptr->GetMaxSize())) {
    return false;
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // std::scoped_lock<std::mutex> lock(latch_);
  auto leaf_page_ptr = FindLeafPage(key, transaction, REMOVE);
  if (leaf_page_ptr == nullptr) {
    return;
  }
  // std::cout << "====================================" << std::endl;
  // std::cout << "Remove start: k: " << key << std::endl;
  // remove before merge
  auto leaf_node_ptr = reinterpret_cast<LeafPage *>(leaf_page_ptr->GetData());
  leaf_node_ptr->Remove(key, comparator_);

  auto cur_page_ptr = leaf_page_ptr;
  while (true) {
    auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
    // no merge
    if (cur_node_ptr->GetSize() >= cur_node_ptr->GetMinSize()) {
      break;
    }
    // 当前是 root page
    if (cur_node_ptr->IsRootPage()) {
      // leaf node，root 就是 leaf，所以不用改变
      if (cur_node_ptr->IsLeafPage()) {
        break;
      }
      // 最少两个
      if (cur_node_ptr->GetSize() >= 2) {
        break;
      }
      // internal node，需要将 child 的 kv 复制到 root node，并删除该 child
      // child -> root
      auto child_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->ValueAt(0));
      child_page_ptr->WLatch();
      auto tmp_child_node_ptr = reinterpret_cast<LeafPage *>(child_page_ptr->GetData());
      if (tmp_child_node_ptr->IsLeafPage()) {
        auto child_node_ptr = tmp_child_node_ptr;
        auto tmp_cur_node_ptr = reinterpret_cast<LeafPage *>(cur_node_ptr);
        tmp_cur_node_ptr->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
        for (int i = 0, sz = child_node_ptr->GetSize(); i < sz; i++) {
          tmp_cur_node_ptr->InsertLast(child_node_ptr->KeyAt(i), child_node_ptr->ValueAt(i));
        }
      } else {
        auto child_node_ptr = reinterpret_cast<InternalPage *>(tmp_child_node_ptr);
        cur_node_ptr->SetSize(0);
        for (int i = 0, sz = child_node_ptr->GetSize(); i < sz; i++) {
          cur_node_ptr->InsertLast(child_node_ptr->KeyAt(i), child_node_ptr->ValueAt(i));
          auto grandson_page_ptr = buffer_pool_manager_->FetchPage(child_node_ptr->ValueAt(i));
          auto grandson_node_ptr = reinterpret_cast<InternalPage *>(grandson_page_ptr->GetData());
          grandson_page_ptr->WLatch();
          grandson_node_ptr->SetParentPageId(cur_node_ptr->GetPageId());
          grandson_page_ptr->WUnlatch();
          buffer_pool_manager_->UnpinPage(grandson_page_ptr->GetPageId(), true);
        }
        cur_node_ptr->SetPageType(IndexPageType::INTERNAL_PAGE);
        cur_node_ptr->SetMaxSize(internal_max_size_);
      }
      child_page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), false);
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(child_page_ptr->GetPageId());
      } else {
        buffer_pool_manager_->DeletePage(child_page_ptr->GetPageId());
      }
      break;
    }
    // 释放 cur_page lock
    if (transaction != nullptr) {
      // auto tmp_page_ptr = transaction->GetPageSet()->back();
      transaction->GetPageSet()->pop_back();
      cur_page_ptr->WUnlatch();
    }
    buffer_pool_manager_->UnpinPage(cur_node_ptr->GetPageId(), true);

    Page *parent_page_ptr = nullptr;
    if (transaction != nullptr) {
      parent_page_ptr = transaction->GetPageSet()->back();
    } else {
      parent_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->GetParentPageId());
      buffer_pool_manager_->UnpinPage(cur_node_ptr->GetParentPageId(), false);
    }
    auto parent_node_ptr = reinterpret_cast<InternalPage *>(parent_page_ptr->GetData());
    // 如果 sibling node 节点充足，则将其一个节点转移到 cur node，
    // 否则，二者合并为一个节点，
    auto cur_node_index = parent_node_ptr->FindChildIndex(cur_node_ptr->GetPageId());

    // 如果当前 page 处于 children 最右边，那么就选择左侧的 page 进行处理
    int index = -1;
    if (cur_node_index == parent_node_ptr->GetSize() - 1) {
      index = cur_node_index - 1;
    } else {
      index = cur_node_index;
    }
    auto left_page_ptr = buffer_pool_manager_->FetchPage(parent_node_ptr->ValueAt(index));
    auto right_page_ptr = buffer_pool_manager_->FetchPage(parent_node_ptr->ValueAt(index + 1));
    left_page_ptr->WLatch();
    right_page_ptr->WLatch();
    auto tmp_left_node_ptr = reinterpret_cast<InternalPage *>(left_page_ptr->GetData());
    auto tmp_right_node_ptr = reinterpret_cast<InternalPage *>(right_page_ptr->GetData());
    // 左边保留 [0, min_size)，右边保留 [min_size, left_size + right_size)
    auto left_size = tmp_left_node_ptr->GetSize();
    auto right_size = tmp_right_node_ptr->GetSize();
    // auto min_size = (left_size + right_size + 1) >> 1;
    // 不需要合并
    if (left_size + right_size >= tmp_left_node_ptr->GetMinSize() * 2) {
      // leaf node
      if (tmp_left_node_ptr->IsLeafPage()) {
        auto left_node_ptr = reinterpret_cast<LeafPage *>(tmp_left_node_ptr);
        auto right_node_ptr = reinterpret_cast<LeafPage *>(tmp_right_node_ptr);
        // right -> left
        if (left_size < right_size) {
          auto key = right_node_ptr->KeyAt(0);
          auto val = right_node_ptr->ValueAt(0);
          left_node_ptr->InsertLast(key, val);
          right_node_ptr->Remove(key, comparator_);
        } else {
          // left -> right
          auto sz = left_node_ptr->GetSize();
          auto key = left_node_ptr->KeyAt(sz - 1);
          auto val = left_node_ptr->ValueAt(sz - 1);
          right_node_ptr->InsertFirst(key, val);
          left_node_ptr->Remove(key, comparator_);
        }
        auto index = parent_node_ptr->FindChildIndex(right_node_ptr->GetPageId());
        parent_node_ptr->SetKeyAt(index, right_node_ptr->KeyAt(0));
      } else {
        // internal node
        auto left_node_ptr = tmp_left_node_ptr;
        auto right_node_ptr = tmp_right_node_ptr;
        // right -> left
        if (left_size < right_size) {
          auto key = right_node_ptr->KeyAt(0);
          auto val = right_node_ptr->ValueAt(0);
          left_node_ptr->InsertLast(key, val);
          auto child_page_ptr = buffer_pool_manager_->FetchPage(val);
          auto child_node_ptr = reinterpret_cast<InternalPage *>(child_page_ptr->GetData());
          child_page_ptr->WLatch();
          if (child_node_ptr->IsLeafPage()) {
            auto tmp_child_node_ptr = reinterpret_cast<LeafPage *>(child_node_ptr);
            right_node_ptr->SetKeyAt(0, tmp_child_node_ptr->KeyAt(0));
          } else {
            right_node_ptr->SetKeyAt(0, child_node_ptr->KeyAt(0));
          }
          child_node_ptr->SetParentPageId(left_node_ptr->GetPageId());
          child_page_ptr->WUnlatch();
          buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
          right_node_ptr->RemoveIndex(0);
        } else {
          // left -> right
          auto sz = left_node_ptr->GetSize();
          auto key = left_node_ptr->KeyAt(sz - 1);
          auto val = left_node_ptr->ValueAt(sz - 1);
          right_node_ptr->InsertFirst(key, val);
          auto child_page_ptr = buffer_pool_manager_->FetchPage(val);
          auto child_node_ptr = reinterpret_cast<InternalPage *>(child_page_ptr->GetData());
          child_page_ptr->WLatch();
          child_node_ptr->SetParentPageId(right_node_ptr->GetPageId());
          child_page_ptr->WUnlatch();
          buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
          left_node_ptr->RemoveIndex(sz - 1);
        }
        auto index = parent_node_ptr->FindChildIndex(right_node_ptr->GetPageId());
        parent_node_ptr->SetKeyAt(index, right_node_ptr->KeyAt(0));
      }
      left_page_ptr->WUnlatch();
      right_page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_page_ptr->GetPageId(), true);
    } else {
      // 需要合并
      // leaf node
      if (tmp_left_node_ptr->IsLeafPage()) {
        auto left_node_ptr = reinterpret_cast<LeafPage *>(tmp_left_node_ptr);
        auto right_node_ptr = reinterpret_cast<LeafPage *>(tmp_right_node_ptr);
        // 全部合并到 left node
        left_node_ptr->SetNextPageId(right_node_ptr->GetNextPageId());
        for (int i = 0; i < right_size; i++) {
          left_node_ptr->InsertLast(right_node_ptr->KeyAt(i), right_node_ptr->ValueAt(i));
        }
        parent_node_ptr->RemoveIndex(parent_node_ptr->FindChildIndex(right_node_ptr->GetPageId()));
      } else {
        // internal node
        auto left_node_ptr = tmp_left_node_ptr;
        auto right_node_ptr = tmp_right_node_ptr;
        for (int i = 0; i < right_size; i++) {
          auto child_page_ptr = buffer_pool_manager_->FetchPage(right_node_ptr->ValueAt(i));
          auto child_node_ptr = reinterpret_cast<InternalPage *>(child_page_ptr->GetData());
          child_page_ptr->WLatch();
          if (child_node_ptr->IsLeafPage()) {
            auto tmp_child_node_ptr = reinterpret_cast<LeafPage *>(child_node_ptr);
            right_node_ptr->SetKeyAt(i, tmp_child_node_ptr->KeyAt(0));
          } else {
            right_node_ptr->SetKeyAt(i, child_node_ptr->KeyAt(0));
          }
          left_node_ptr->InsertLast(right_node_ptr->KeyAt(i), right_node_ptr->ValueAt(i));
          child_node_ptr->SetParentPageId(left_node_ptr->GetPageId());
          child_page_ptr->WUnlatch();
          buffer_pool_manager_->UnpinPage(child_page_ptr->GetPageId(), true);
        }
        parent_node_ptr->RemoveIndex(parent_node_ptr->FindChildIndex(right_node_ptr->GetPageId()));
      }
      left_page_ptr->WUnlatch();
      right_page_ptr->WUnlatch();
      buffer_pool_manager_->UnpinPage(tmp_right_node_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(tmp_left_node_ptr->GetPageId(), true);
      // 移除 right node
      if (transaction != nullptr) {
        transaction->AddIntoDeletedPageSet(tmp_right_node_ptr->GetPageId());
      } else {
        buffer_pool_manager_->DeletePage(tmp_right_node_ptr->GetPageId());
      }
      // tmp_left_node_ptr->SetSize(left_size + right_size);
    }
    cur_page_ptr = parent_page_ptr;
  }
  if (transaction != nullptr && transaction->GetPageSet()->empty()) {
    throw Exception("impossible");
  }
  // Print(buffer_pool_manager_);
  // std::cout << std::endl;
  if (transaction == nullptr) {
    UnpinFromBottomToRoot(cur_page_ptr->GetPageId(), buffer_pool_manager_, REMOVE);
  } else {
    UnpinAndUnlock(transaction, REMOVE);
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, buffer_pool_manager_);
  }
  auto cur_page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  cur_page_ptr->RLatch();
  auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
  while (!cur_node_ptr->IsLeafPage()) {
    auto next_page_id = cur_node_ptr->ValueAt(0);
    auto next_page_ptr = buffer_pool_manager_->FetchPage(next_page_id);
    auto next_node_ptr = reinterpret_cast<InternalPage *>(next_page_ptr->GetData());
    next_page_ptr->RLatch();
    cur_page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_ptr->GetPageId(), false);
    cur_node_ptr = next_node_ptr;
    cur_page_ptr = next_page_ptr;
  }
  // 如果 tree 为空
  if (cur_node_ptr->GetSize() == 0) {
    return End();
  }
  return INDEXITERATOR_TYPE(cur_page_ptr, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  // 下面类似 FindLeafPage
  auto cur_page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
  if (cur_page_ptr == nullptr) {
    return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, buffer_pool_manager_);
  }
  cur_page_ptr->RLatch();
  auto cur_node_ptr = reinterpret_cast<InternalPage *>(cur_page_ptr->GetData());
  while (!cur_node_ptr->IsLeafPage()) {
    // fetch page
    auto next_page_ptr = buffer_pool_manager_->FetchPage(cur_node_ptr->FindChild(key, comparator_));
    next_page_ptr->RLatch();
    cur_page_ptr->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_ptr->GetPageId(), false);
    auto next_node_ptr = reinterpret_cast<InternalPage *>(next_page_ptr->GetData());
    cur_node_ptr = next_node_ptr;
    cur_page_ptr = next_page_ptr;
  }
  auto leaf_node_ptr = reinterpret_cast<LeafPage *>(cur_node_ptr);
  // 如果 tree 为空
  if (cur_node_ptr->GetSize() == 0) {
    return End();
  }
  return INDEXITERATOR_TYPE(cur_page_ptr, leaf_node_ptr->KeyIndexOf(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  return INDEXITERATOR_TYPE(nullptr, INVALID_PAGE_ID, buffer_pool_manager_);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
