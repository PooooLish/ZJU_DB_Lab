#include "executor/executors/update_executor.h"
#include "common/macros.h"
UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
    auto table_name=plan->GetTableName();
    exec_ctx->GetCatalog()->GetTable(table_name, table_info_);  //get table_info_
    //get all the indexes on this table

}

/**
* TODO: Student Implement
 */
void UpdateExecutor::Init() {
    child_executor_->Init();
    exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), table_indexes_);
}

bool UpdateExecutor::Next(Row *row, RowId *rid) {
    Row src_row;
    RowId src_rid;

    if(child_executor_->Next(&src_row,&src_rid)){
        // ASSERT(src_row.GetRowId().Get()!=INVALID_ROWID.Get(),"Update Invalid Row");
        Row dest_row =GenerateUpdatedTuple(src_row);  //generate new row
        if(!table_info_->GetTableHeap()->UpdateTuple(dest_row, src_rid, exec_ctx_->GetTransaction())) return false;
        for(auto& index_info: table_indexes_){ //update all the indexes
            Row key;
            row->GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key);
            index_info->GetIndex()->RemoveEntry(key, *rid, exec_ctx_->GetTransaction());
            dest_row.GetKeyFromRow(table_info_->GetSchema(), index_info->GetIndexKeySchema(), key);
            index_info->GetIndex()->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
        }
        return true;
    }
    return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row) {
    auto& attrs=plan_->GetUpdateAttr(); //Map from column index -> update operation
    uint32_t col_cnt=table_info_->GetSchema()->GetColumnCount();
    std::vector<Field> fields;
    for(uint32_t i=0; i<col_cnt; i++){
        if(attrs.find(i)==attrs.end()) fields.emplace_back(*src_row.GetField(i)); //not fonud, which means no update
        else{ //has update
            Field f=attrs.at(i)->Evaluate(nullptr); //get new field
            fields.emplace_back(f);
        }
    }
    return Row(fields);
}