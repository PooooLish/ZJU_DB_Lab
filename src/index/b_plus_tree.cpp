#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  if (leaf_max_size_ == UNDEFINED_SIZE) {
    leaf_max_size_ = (int)((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1);
  }
  if (internal_max_size_ == UNDEFINED_SIZE) {
    internal_max_size_ = (int)((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)) - 1);
  }
  auto root_page =
      reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));  // fetch root page
  root_page->GetRootId(index_id, &root_page_id_);                                               // get root page id
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

void BPlusTree::Destroy(page_id_t current_page_id) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction) {
  if (IsEmpty()) {  // empty tree
    return false;
  }
  Page *leaf_page = FindLeafPage(key);  // find leaf page
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  RowId value;
  if (node->Lookup(key, value, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    result.emplace_back(value);
    return true;
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  Page *new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(new_root_page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  leaf_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
  UpdateRootPageId(1);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction) {
  Page *leaf_page = FindLeafPage(key);  // find leaf page
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int size = node->GetSize();                           // get current size
  int cur_size = node->Insert(key, value, processor_);  // insert
  if (cur_size == size) {                               // 如果插入失败
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  } else {
    if (cur_size < leaf_max_size_) {  // 如果插入成功且未满
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return true;
    } else {
      BPlusTreeLeafPage *next_page = Split(node, transaction);  // 如果插入成功且满了
      next_page->SetNextPageId(node->GetNextPageId());          // 设置next page id
      node->SetNextPageId(next_page->GetPageId());
      InsertIntoParent(node, next_page->KeyAt(0), next_page, transaction);
      buffer_pool_manager_->UnpinPage(next_page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
      return true;
    }
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction) {
  page_id_t t;
  Page *new_page = buffer_pool_manager_->NewPage(t);
  InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
  new_node->Init(new_page->GetPageId(), node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction) {
  page_id_t t;
  Page *new_page = buffer_pool_manager_->NewPage(t);
  LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_node->Init(new_page->GetPageId(), node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_node);
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  return new_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *new_root_page = buffer_pool_manager_->NewPage(root_page_id_);
    InternalPage *new_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    new_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_page->GetPageId());
    new_node->SetParentPageId(new_page->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    UpdateRootPageId(0);
    return;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *new_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int new_size = new_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  if (new_size < internal_max_size_) {
    old_node->SetParentPageId(new_page->GetPageId());
    new_node->SetParentPageId(new_page->GetPageId());
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  } else {
    InternalPage *next_node = Split(new_page, transaction);
    InsertIntoParent(new_page, next_node->KeyAt(0), next_node);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(next_node->GetPageId(), true);
    return;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction) {
  if (IsEmpty())                                                           // 如果根节点为空
    return;
  Page *now_page = FindLeafPage(key);  // 查找叶子页
  LeafPage *leaf = reinterpret_cast<LeafPage *>(now_page->GetData());
  if (!now_page)  // 如果叶子页为空
    return;
  int size_before = leaf->GetSize();
  int size_after = leaf->RemoveAndDeleteRecord(key, processor_);  // 删除
  if (size_after == size_before) {                                // 如果pair未删除
    buffer_pool_manager_->UnpinPage(now_page->GetPageId(), false);
    return;
  } else {
    if (!CoalesceOrRedistribute(leaf, transaction)) {  // 合并或者重分布
      buffer_pool_manager_->UnpinPage(now_page->GetPageId(), true);
      return;
    } else {
      buffer_pool_manager_->UnpinPage(now_page->GetPageId(), true);
      buffer_pool_manager_->DeletePage(now_page->GetPageId());  // 删除页
      return;
    }
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction) {
  if (node->IsRootPage()) 
    return AdjustRoot(node);
  else {
    if (node->GetSize() >= node->GetMinSize()) {
      return false;
    }
    Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    InternalPage *par_page = reinterpret_cast<InternalPage *>(page->GetData());
    int index = par_page->ValueIndex(node->GetPageId());
    int neighbor_id = (index == 0) ? 1 : index - 1;
    Page *neighbor_page = buffer_pool_manager_->FetchPage((par_page->ValueAt(neighbor_id)));
    auto *neighbor_node = reinterpret_cast<N *>(neighbor_page->GetData());
    if (neighbor_node->GetSize() + node->GetSize() >= node->GetMinSize()) {
      Redistribute(neighbor_node, node, index);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
      return false;
    } else {
      if (Coalesce(neighbor_node, node, par_page, index)) {
        buffer_pool_manager_->UnpinPage(par_page->GetPageId(), true);
        buffer_pool_manager_->DeletePage(par_page->GetPageId());
      }
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
      return true;
    }
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if (index == 0) {
    index = 1;
    neighbor_node->MoveAllTo(node); // 将neighbor_node的所有pair移动到node
    buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    parent->Remove(index); // 删除parent中的index
    return CoalesceOrRedistribute(parent, transaction); // 合并或者重分布
  }
  node->MoveAllTo(neighbor_node); // 将node的所有pair移动到neighbor_node
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  parent->Remove(index);
  return CoalesceOrRedistribute(parent, transaction);
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction) {
  if (index == 0) {
    index = 1;
    swap(neighbor_node, node);
  }
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_); // 将node的所有pair移动到neighbor_node
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);
  return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  Page *par_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *par_node = reinterpret_cast<InternalPage *>(par_page->GetData());
  if (index == 0) { // neighbor_node在node的左边
    neighbor_node->MoveFirstToEndOf(node); // 将neighbor_node的第一个pair移动到node的最后
    par_node->SetKeyAt(1, neighbor_node->KeyAt(0)); // 将neighbor_node的第一个key设置为parent的第二个key
  } else {
    neighbor_node->MoveLastToFrontOf(node); // 将neighbor_node的最后一个pair移动到node的最前
    par_node->SetKeyAt(index, node->KeyAt(0)); // 将node的第一个key设置为parent的第index个key
  }
  buffer_pool_manager_->UnpinPage(par_page->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  Page *par_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *par_node = reinterpret_cast<InternalPage *>(par_page->GetData());
  if (index == 0) { // neighbor_node在node的左边
    neighbor_node->MoveFirstToEndOf(node, par_node->KeyAt(1), buffer_pool_manager_); // 将neighbor_node的第一个pair移动到node的最后
    par_node->SetKeyAt(1, neighbor_node->KeyAt(0)); // 将neighbor_node的第一个key设置为parent的第二个key
  } else {
    neighbor_node->MoveLastToFrontOf(node, par_node->KeyAt(index), buffer_pool_manager_); // 将neighbor_node的最后一个pair移动到node的最前
    par_node->SetKeyAt(index, node->KeyAt(0)); // 将node的第一个key设置为parent的第index个key
  }
  buffer_pool_manager_->UnpinPage(par_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) { // 不是leaf，且只有一个pair
    InternalPage *root = reinterpret_cast<InternalPage *>(old_root_node);
    Page *child_page = buffer_pool_manager_->FetchPage(root->ValueAt(0));
    auto *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(INVALID_PAGE_ID); 
    root_page_id_ = child_node->GetPageId(); // 将child_node设置为root
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
    UpdateRootPageId(0); 
    return true;
  }
  return old_root_node->IsLeafPage() && old_root_node->GetSize() == 1; // 只有一个pair，且是leaf
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  GenericKey *key;
  Page *leaf_page = FindLeafPage(key, root_page_id_, true); // 找到最左边的leaf
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, 0); 
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page *leaf_page = FindLeafPage(key); // 找到包含key的leaf
  LeafPage *node = reinterpret_cast<LeafPage *>(leaf_page->GetData()); 
  int temp_index = node->KeyIndex(key, processor_); // 找到key在leaf中的index
  return IndexIterator(leaf_page->GetPageId(), buffer_pool_manager_, 0);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { // 找到最右边的leaf  
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_); // 找到root
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData()); // 找到root的第一个child
  while (!node->IsLeafPage()) { 
    page_id_t child_id; 
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(page->GetData());
    child_id = internal_node->ValueAt(internal_node->GetSize() - 1); // 找到最右边的child
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_id); 
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return IndexIterator(page->GetPageId(), buffer_pool_manager_, node->GetSize()); // 返回最右边leaf的最后一个pair
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (IsEmpty()) 
    return nullptr;
  Page *page = buffer_pool_manager_->FetchPage(root_page_id_); 
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) { 
    page_id_t child_id;
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(page->GetData());
    if (leftMost) { // 找到最左边的leaf
      child_id = internal_node->ValueAt(0); 
    } else {
      child_id = internal_node->Lookup(key, processor_);
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(child_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID); // 找到header page
  auto *root_page = reinterpret_cast<IndexRootsPage *>(page->GetData());
  if (insert_record == 0) { // 如果插入记录
    root_page->Update(index_id_, root_page_id_);
  } else {
    root_page->Insert(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
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
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
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
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}