#include "../include/catalog/catalog.h"


void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
    // Calculate the size of serialized data
    uint32_t size = sizeof(uint32_t);  // For magic number

    // Add the size of table meta pages
    size += sizeof(uint32_t);
    for (const auto& entry : table_meta_pages_) {
        size += sizeof(table_id_t);
        size += sizeof(page_id_t);
    }

    // Add the size of index meta pages
    size += sizeof(uint32_t);
    for (const auto& entry : index_meta_pages_) {
        size += sizeof(index_id_t);
        size += sizeof(page_id_t);
    }

    return size;
}


CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
        : buffer_pool_manager_(buffer_pool_manager),
          lock_manager_(lock_manager),
          log_manager_(log_manager),
          catalog_meta_(nullptr),
          next_table_id_(0),
          next_index_id_(0) {
    if (init) {

        catalog_meta_ = CatalogMeta::NewInstance();
    } else {

        Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(meta_page->GetData());

        for (auto it : *catalog_meta_->GetTableMetaPages()) {
            page_id_t table_meta_page_id = it.second;
            Page *table_meta_page = buffer_pool_manager_->FetchPage(table_meta_page_id);
            char *table_meta_page_data = table_meta_page->GetData();

            TableMetadata *table_meta;
            TableMetadata::DeserializeFrom(table_meta_page_data, table_meta);
            TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(),
                                                      log_manager_, lock_manager_);

            table_names_[table_meta->GetTableName()] = table_meta->GetTableId();

            TableInfo *table_info = TableInfo::Create();
            table_info->Init(table_meta, table_heap);

            // Store table info
            tables_[table_meta->GetTableId()] = table_info;

            // Unpin table meta page
            buffer_pool_manager_->UnpinPage(table_meta_page_id, false);
        }

        for (auto it : *catalog_meta_->GetIndexMetaPages()) {
            page_id_t index_meta_page_id = it.second;

            Page *index_meta_page = buffer_pool_manager_->FetchPage(index_meta_page_id);
            char *index_meta_page_data = index_meta_page->GetData();
            IndexMetadata *index_meta;

            IndexMetadata::DeserializeFrom(index_meta_page_data, index_meta);

            std::string table_name = tables_[index_meta->GetTableId()]->GetTableName();
            std::string index_name = index_meta->GetIndexName();

            index_names_[table_name][index_name] = index_meta->GetIndexId();

            IndexInfo *index_info = IndexInfo::Create();
            index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);

            indexes_[index_meta->GetIndexId()] = index_info;

            buffer_pool_manager_->UnpinPage(index_meta_page_id, false);
        }

//        next_table_id_.store(catalog_meta_->GetNextTableId() + 1);
//        next_index_id_.store(catalog_meta_->GetNextIndexId() + 1);


        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    }
}


CatalogManager::~CatalogManager() {
 /** After you finish the code for the CatalogManager section,
 *  you can uncomment the commented code. Otherwise it will affect b+tree test **/
  FlushCatalogMetaPage();
  delete catalog_meta_;

//  for (auto iter : tables_) {
//    delete iter.second;
//  }
//  for (auto iter : indexes_) {
//    delete iter.second;
//  }


}

/**
* TODO: Student Implement
*/
dberr_t CatalogManager::CreateTable(const std::string& table_name, TableSchema* schema, Transaction* txn,
                                    TableInfo*& table_info) {
    // Check if the table already exists
    if (table_names_.find(table_name) != table_names_.end()) {
        return DB_TABLE_ALREADY_EXIST;
    }

    // Create a new table metadata
    page_id_t table_meta_page_id;
    Page* meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
    TableHeap* table_heap = TableHeap::Create(buffer_pool_manager_, schema, nullptr, log_manager_, lock_manager_);
    TableMetadata* table_meta = TableMetadata::Create(next_table_id_, table_name, table_heap->GetFirstPageId(), schema);

    table_meta->SerializeTo(meta_page->GetData());
    buffer_pool_manager_->UnpinPage(meta_page->GetPageId(), true);

    // Create a new table info
    table_info = TableInfo::Create();
    table_info->Init(table_meta, table_heap);

    // Add the table to the catalog
    table_names_[table_name] = next_table_id_;
    tables_[next_table_id_] = table_info;
    catalog_meta_->table_meta_pages_[next_table_id_] = table_meta_page_id;

    // Increment the next table id
    next_table_id_++;

    return DB_SUCCESS;
}





/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const std::string& table_name, TableInfo*& table_info) {
    // Check if the table name exists in the map
    if (table_names_.find(table_name) == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    table_id_t table_id = table_names_[table_name];

    if (tables_.find(table_id) == tables_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    table_info = tables_[table_id];

    return DB_SUCCESS;
}




/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo*>& tables) const {
    tables.clear();
    if(tables_.empty()) {
        return DB_FAILED;
    }

    for (const auto& entry : tables_) {
        tables.push_back(entry.second);
    }

    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const string &table_name, const string &index_name, const vector<std::string> &index_keys,
                                    Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {

    TableInfo *table_info;
    dberr_t result = GetTable(table_name, table_info);
    if (result != DB_SUCCESS) {
        return result;
    }
    if(table_names_.count(table_name) <= 0 ) {
        return DB_TABLE_NOT_EXIST;
    }

    if (index_names_.count(table_name) > 0 && index_names_[table_name].count(index_name) > 0) {
        return DB_INDEX_ALREADY_EXIST;
    }

    std::vector<uint32_t> key_map;
    for (const auto &key : index_keys) {
        uint32_t column_index;
        result = table_info->GetSchema()->GetColumnIndex(key, column_index);
        if (result != DB_SUCCESS) {
            return result;
        }
        key_map.push_back(column_index);
    }

    page_id_t page_id;

    Page* index_meta_page = buffer_pool_manager_->NewPage(page_id);
    catalog_meta_->index_meta_pages_[catalog_meta_->GetNextIndexId()] = page_id;
    buffer_pool_manager_->UnpinPage(page_id, true);

    IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_, index_name, table_info->GetTableId(), key_map);
    if (index_meta == nullptr) {
        return DB_FAILED;
    }

    index_meta->SerializeTo(index_meta_page->GetData());

    IndexSchema *index_schema = Schema::ShallowCopySchema(table_info->GetSchema(), key_map);
    if (index_schema == nullptr) {
        delete index_meta;
        return DB_FAILED;
    }

    index_info = IndexInfo::Create();
    if (index_info == nullptr) {
        delete index_meta;
        delete index_schema;
        return DB_FAILED;
    }

    index_info->Init(index_meta, table_info, buffer_pool_manager_);
//    string index_type = "type";
//    Index *index = index_info->CreateIndex(buffer_pool_manager_, index_type);
//    if (index == nullptr) {
//        delete index_info;
//        return DB_FAILED;
//    }


    next_index_id_++;
    index_names_[table_name][index_name] = index_meta->GetIndexId();
    indexes_[index_meta->GetIndexId()] = index_info;

    return DB_SUCCESS;
}



/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    auto table_it = table_names_.find(table_name);
    if (table_it == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    auto index_name_it = index_names_.find(table_name);
    if (index_name_it == index_names_.end()) {
        return DB_INDEX_NOT_FOUND;
    }

    auto index_id_it = index_name_it->second.find(index_name);
    if (index_id_it == index_name_it->second.end()) {
        return DB_INDEX_NOT_FOUND;
    }

    index_id_t index_id = index_id_it->second;
    auto index_info_it = indexes_.find(index_id);
    if (index_info_it == indexes_.end()) {
        return DB_INDEX_NOT_FOUND;
    }

    index_info = index_info_it->second;
    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    // Check if the table exists
    if (table_names_.find(table_name) == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    table_id_t table_id = table_names_.at(table_name);
    TableInfo* table_info = tables_.at(table_id);

    auto index_entries = index_names_.find(table_name);
    if (index_entries == index_names_.end()) {
        // No indexes found for the table
        return DB_SUCCESS;
    }

    for (const auto &index_entry : index_entries->second) {
        const std::string &index_name = index_entry.first;
        index_id_t index_id = index_entry.second;

        auto index_meta_entry = indexes_.find(index_id);
        if (index_meta_entry == indexes_.end()) {
            return DB_INDEX_NOT_FOUND;
        }
        IndexMetadata *index_meta = index_meta_entry->second->meta_data_;

        IndexInfo *index_info = IndexInfo::Create();
        index_info->Init(index_meta, table_info, buffer_pool_manager_);

        indexes.push_back(index_info);
    }

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const std::string &table_name) {

    if (table_names_.find(table_name) == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    table_id_t table_id = table_names_[table_name];

    std::vector<IndexInfo *> indexes;
    GetTableIndexes(table_name, indexes);
    for (auto index_info : indexes) {
        DropIndex(table_name, index_info->GetIndexName());
    }

    // Delete the table metadata page
    if (tables_.find(table_id) == tables_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    tables_.erase(table_id);

    table_names_.erase(table_name);

    page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
    buffer_pool_manager_->DeletePage((page_id));

    FlushCatalogMetaPage();

    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {

    if (table_names_.find(table_name) == table_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    if (index_names_.find(table_name) == index_names_.end() ||
        index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
        return DB_INDEX_NOT_FOUND;
    }


    index_id_t index_id = index_names_[table_name][index_name];


    if (!catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id)) {
        return DB_FAILED;
    }

    index_names_[table_name].erase(index_name);

    page_id_t  page_id = catalog_meta_->index_meta_pages_[index_id];
    buffer_pool_manager_->DeletePage(page_id);

    indexes_.erase(index_id);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {

    uint32_t serialized_size = catalog_meta_->GetSerializedSize();

    char *buffer = new char[serialized_size];

    catalog_meta_->SerializeTo(buffer);

    Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    if (page == nullptr) {
        delete[] buffer;
        return DB_FAILED;
    }

    memcpy(page->GetData(), buffer, serialized_size);


    delete[] buffer;

    if(!buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
        return DB_FAILED;
    }

    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {

    if (tables_.find(table_id) != tables_.end()) {
        return DB_TABLE_ALREADY_EXIST;
    }

    catalog_meta_->table_meta_pages_[table_id] = page_id;

    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
        return DB_NOT_EXIST;
    }

    TableMetadata *meta_data;
    meta_data->DeserializeFrom(page->GetData(), meta_data);
    table_names_[meta_data->GetTableName()] = table_id;

    TableInfo *table_info = TableInfo::Create();
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, page_id, meta_data->GetSchema(), log_manager_, lock_manager_);

    table_info->Init(meta_data, table_heap);

    tables_.emplace(table_id, table_info);

    catalog_meta_->table_meta_pages_[table_id] = page_id;
    return DB_SUCCESS;
}



/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {

    if(indexes_.find(index_id) != indexes_.end()) {
        return DB_INDEX_ALREADY_EXIST;
    }

    catalog_meta_->index_meta_pages_[index_id] = page_id;
    Page* meta_page = buffer_pool_manager_->FetchPage(page_id);


    IndexMetadata* index_meta;
    uint32_t result = IndexMetadata::DeserializeFrom(meta_page->GetData(), index_meta);
    if (result != 0) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return DB_FAILED;
    }

    TableInfo* table_info;
    result = GetTable(index_meta->GetTableId(), table_info);
    if (result != DB_SUCCESS) {
        buffer_pool_manager_->UnpinPage(page_id, false);
        return DB_FAILED;
    }

    IndexInfo* index_info = IndexInfo::Create();

    index_info->Init(index_meta, table_info, buffer_pool_manager_);


    indexes_[index_id] = index_info;
    catalog_meta_->index_meta_pages_[index_id] = page_id;

    return DB_SUCCESS;
}


/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it = tables_.find(table_id);
    if (it == tables_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    table_info = it->second;
    return DB_SUCCESS;
}
