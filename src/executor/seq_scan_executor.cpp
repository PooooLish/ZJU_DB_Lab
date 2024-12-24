//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
* TODO: Student Implement
*/
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
        : AbstractExecutor(exec_ctx), plan_(plan) {
    TableInfo *temp;
    exec_ctx->GetCatalog()->GetTable(plan->GetTableName(), temp);
    table_info_ = temp;
}

void SeqScanExecutor::Init() { table_iter_ = table_info_->GetTableHeap()->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
    do{
        if(table_iter_ == table_info_->GetTableHeap()->End())
            return false;
        *row = *table_iter_;
        *rid = row->GetRowId();
        table_iter_++;
    }while(plan_->GetPredicate() != nullptr && !plan_->GetPredicate()->Evaluate(row).CompareEquals(Field(kTypeInt, 1)));

    auto schema = plan_->OutputSchema();
    std::vector<Field> values;
    for(auto &col : schema->GetColumns()){
        uint32_t idx;
        schema->GetColumnIndex(col->GetName(),idx);
        values.emplace_back(*row->GetField(idx));
    }
    *row = Row{values};
    // row->SetRowId(*rid);
    return true;
}
