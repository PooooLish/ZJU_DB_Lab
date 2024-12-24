#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  bool insert_success= false;
  if(row.GetSerializedSize(schema_)>TablePage::SIZE_MAX_ROW) return false;
  if(first_page_id_==INVALID_PAGE_ID){
    auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(first_page_id_));
    ASSERT(page!= nullptr,"first page allocation failed.");
    page->WLatch();
    page->Init(first_page_id_,INVALID_PAGE_ID,log_manager_,txn);
    insert_success = page->InsertTuple(row,schema_,txn, lock_manager_,log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, insert_success);
  }else{
    page_id_t page_id = first_page_id_;
    TablePage* page;
    while(page_id!=INVALID_PAGE_ID){
      page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
      ASSERT(page!= nullptr,"page fetch failed.");
      page->WLatch();
      insert_success = page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
      if(!insert_success) {
        if(page->GetNextPageId()==INVALID_PAGE_ID){
          page_id_t new_page_id;
          //cout<<"need to allocate a new page"<<endl;
          auto new_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
          ASSERT(new_page!= nullptr,"new page allocation failed.");
          new_page->WLatch();
          new_page->Init(new_page_id,page_id,log_manager_,txn);
          page->SetNextPageId(new_page_id);
          insert_success = new_page->InsertTuple(row,schema_,txn,lock_manager_,log_manager_);
          new_page->WUnlatch();
          page->WUnlatch();
          buffer_pool_manager_->UnpinPage(new_page_id,insert_success);
          break;
        }else page_id = page->GetNextPageId();
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id,insert_success);
      }else{
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id,insert_success);
        break;
      }
    }
  }
  return insert_success;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) // 没有数据页
    return false;
  page->WLatch();
  Row old_row(rid);
  int insert_success = page->UpdateTuple(row, &old_row, this->schema_, txn, lock_manager_, log_manager_);
  if(insert_success == 0){
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }
  else if(insert_success == 1){ // slotId越界，没有该记录, 返回错误, 不更新
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  else if(insert_success == 2){ // 标记删除 or 物理删除, 不更新
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  else if(insert_success == 3){ // 数据页空间不够, 先删除再插入, 自然RowId会变
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    this->InsertTuple(row, txn);
    return true;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page!= nullptr){
    page->WLatch();
    page->ApplyDelete(rid,txn,log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  bool get_success = false;
  if(page!= nullptr){
    page->RLatch();
    get_success = page->GetTuple(row,schema_,txn,lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  return get_success;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  return TableIterator();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator();
}
