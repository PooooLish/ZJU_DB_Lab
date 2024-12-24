#include "executor/executors/index_scan_executor.h"
#include "planner/expressions/logic_expression.h"
#include "planner/expressions/comparison_expression.h"
#include "gtest/gtest.h"
/**
* TODO: Student Implement
 */
bool cmp(const RowId& A, const RowId& B){
    return A.Get() < B.Get();
}

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
        : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(),table_info);
    need_seq.clear();
    RowSet.clear();//init

    std::vector<single_predicate> pred;
    getPredicate(pred,plan_->filter_predicate_.get());
    IndexInfo* temp;
    // cerr<<"IndexScanExecutor Init sizeof pred="<<pred.size()<<'\n';
    for(auto fiet:pred)
    {
        for(auto index_info:plan_->indexes_)
        {
            if(fiet.col_ind_ == *(index_info->GetKeyMapping().begin()))
            {
                temp=index_info;
                break;
            }
        }
        // cerr<<"IndexScanExecutor Init looop1"<<temp->GetIndexName()<<"\n";
        std::vector<Field> fie;
        // for(uint32_t i=0;i<GetOutputSchema()->GetColumnCount();i++)//build a row for scan
        // {
        //     if(i==fiet.col_ind_) fie.push_back(fiet.val_);
        //     // else fie.push_back(Field(TypeId::kTypeInvalid));//empty
        // }
        fie.push_back(fiet.val_);
        // cerr<<"IndexScanExecutor Init looop2 row_size="<<fie.size()<<"\n";
        Row row(fie);
        std::vector<RowId> res;
        temp->GetIndex()->ScanKey(row,res,exec_ctx_->GetTransaction(),fiet.op_);
        // cerr<<"IndexScanExecutor Init looop3 res "<<res[0].GetPageId()<<' '<<res[0].GetSlotNum()<<"\n";
        sort(RowSet.begin(),RowSet.end(),cmp);
        sort(res.begin(),res.end(),cmp);
        auto ret=res;
        ret.clear();
        if(RowSet.size()==0) RowSet=res;
        else {std::set_intersection(begin(res),end(res),begin(RowSet),end(RowSet),insert_iterator<vector<RowId>>(ret,ret.begin()),cmp);
            RowSet=ret;}
    }
    // cerr<<"IndexScanExecutor Init to begin\n";
    itr=RowSet.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
    // cerr<<"IndexScanExecutor Next\n";
    // cerr<<(itr-RowSet.begin())<<'\n';
    if(itr==RowSet.end()) return false;
    if(!plan_->need_filter_)
    {
        // cerr<<"IndexScanExecutor Next not need filter\n";
        *rid=*itr;
        // cerr<<"IndexScanExecutor Next after1\n";
        Row new_row(*rid),res_row;
        // cerr<<"IndexScanExecutor Next after2\n";
        assert(table_info!=nullptr);
        // cerr<<"IndexScanExecutor Next row "<<row->GetRowId().GetPageId()<<' '<<row->GetRowId().GetSlotNum()<<'\n';
        table_info->GetTableHeap()->GetTuple(&new_row,exec_ctx_->GetTransaction());
        new_row.GetKeyFromRow(table_info->GetSchema(),plan_->OutputSchema(),res_row);
        // cerr<<"IndexScanExecutor Next after3\n";
        itr++;
        *row=res_row;
        return true;
    }
    else
    {
        // cerr<<"IndexScanExecutor Next need filter\n";
        while(1)
        {
            if(itr==RowSet.end()) return false;
            bool sign=true;
            *rid=*itr;
            // row->SetRowId(*rid);
            Row new_row(*rid),res_row;
            table_info->GetTableHeap()->GetTuple(&new_row,exec_ctx_->GetTransaction());
            new_row.GetKeyFromRow(table_info->GetSchema(),plan_->OutputSchema(),res_row);
            for(auto it:need_seq)//check every predicate which has no index
            {
                Field res=it->Evaluate(&new_row);
                if(res.CompareEquals(Field(kTypeInt, 1))!=kTrue)
                {
                    sign=false;
                    break;//not match
                }
            }
            itr++;
            if(sign)
            {
                *row=res_row;
                return true;
            }
        }
    }
}

void IndexScanExecutor::getPredicate(std::vector<single_predicate>& pred,AbstractExpression* exp)
{
    LogicExpression* loc=dynamic_cast<LogicExpression*>(exp);
    if(loc!=nullptr)//cast successful
    {
        // cerr<<"getPredicate: Logical\n";
        getPredicate(pred,loc->GetChildAt(0).get());
        getPredicate(pred,loc->GetChildAt(1).get());
    }
    else
    {
        // cerr<<"getPredicate: Non Logical\n";
        int i;
        ComparisonExpression* com=dynamic_cast<ComparisonExpression*>(exp);
        uint32_t col_ind;
        string op;
        if(com->GetChildAt(0)->GetType()==ExpressionType::ColumnExpression)
        {
            col_ind=(dynamic_cast<ColumnValueExpression*>(com->GetChildAt(0).get()))->GetColIdx();
            i=1;
        }
        else
        {
            col_ind=(dynamic_cast<ColumnValueExpression*>(com->GetChildAt(1).get()))->GetColIdx();
            i=0;
        }
        // cerr<<"getPredicate: need filter "<<plan_->need_filter_<<'\n';

        if(plan_->need_filter_)
        {
            bool sign=false;
            for(auto index_info:plan_->indexes_)
            {
                if(col_ind==*(index_info->GetKeyMapping().begin())) sign=true;//has index
            }
            if(!sign)//no index
            {
                need_seq.push_back(com);
                return;
            }
        }
        // cerr<<"getPredicate: has index\n";
        Field val(com->GetChildAt(i)->Evaluate(nullptr));//if has index,add to pred
        op=com->GetComparisonType();
        single_predicate new_pre(col_ind,val,op);
        pred.push_back(new_pre);
    }
    return ;
}