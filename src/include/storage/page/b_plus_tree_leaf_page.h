//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <utility>
#include <vector>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 28
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 28 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * | ParentPageId (4) | PageId (4) | NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = LEAF_PAGE_SIZE - 1);
  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  auto ValueAt(int index) const -> ValueType;

  // 找 key 的 index, INVALID_PAGE_ID if non-exist
  auto KeyIndexOf(const KeyType &key, const KeyComparator &keyComparator) -> int;

  // 将 kv 插入到最后一个
  void InsertLast(const KeyType &key, const ValueType &value);
  // 将 kv 插入到第一个，也就是 array_[0]
  void InsertFirst(const KeyType &key, const ValueType &value);

  /**
   * 进行 split，这里选择分裂的位置为 min_size，旧 page 为分裂后左边的 page
   * [0, min_size) 为左边的 page(原)，[min_size, max_size) 为右边的 page(新)
   * 旧 page 已获取 lock，返回 new page
   */
  auto SplitL(const KeyComparator &keyComparator, BufferPoolManager *buffer_pool_manager_) -> Page *;

  /**
   * 插入 kv 对
   *
   * @param key k
   * @param value v
   * @return true if success, false otherwise(eg: duplicate key)
   */
  auto InsertL(const KeyType &key, const ValueType &value, const KeyComparator &keyComparator) -> bool;

  void Remove(const KeyType &key, const KeyComparator &keyComparator);

  // 检测 key 是否时 duplicate
  // 调用前需要获取该 page lock
  auto IsDuplicateKeyL(const KeyType &key, const KeyComparator &keyComparator) -> bool;

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
