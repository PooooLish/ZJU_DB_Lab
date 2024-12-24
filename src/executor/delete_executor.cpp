//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

/**
* TODO: Student Implement
*/

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info_);

}

void DeleteExecutor::Init() {
    child_executor_->Init();
    exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(),table_indexes_);
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    Row to_delete_row;
    RowId to_delete_rid;
    int32_t cnt = 0;

    while(child_executor_->Next(&to_delete_row,&to_delete_rid)){
        bool deleted = table_info_->GetTableHeap()->MarkDelete(to_delete_rid, exec_ctx_->GetTransaction()); //Mark or actual?
        if(!deleted){
            return false;
        }
        for(auto index : table_indexes_){
            Row key;
            to_delete_row.GetKeyFromRow(table_info_->GetSchema(),index->GetIndexKeySchema(),key);
            index->GetIndex()->RemoveEntry(key,to_delete_rid,exec_ctx_->GetTransaction());
        }
        cnt++;
    }

    std::vector<Field> values{};
    values.emplace_back(TypeId::kTypeInt,cnt);
    *row = Row{values};
    return false;
}