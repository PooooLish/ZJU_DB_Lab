#pragma once

#include <vector>

#include "executor/execute_context.h"
#include "executor/executors/abstract_executor.h"
#include "executor/plans/index_scan_plan.h"
#include "planner/expressions/column_value_expression.h"
#include "planner/expressions/comparison_expression.h"

/**
 * The IndexScanExecutor executor can over a table.
 */
class single_predicate{
public:
    uint32_t col_ind_;
    Field val_;
    string op_;

    single_predicate(int col_ind,Field& val,string& op):col_ind_(col_ind),val_(val),op_(op) {}
    ~single_predicate()=default;
};

class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next row from the sequential scan.
   * @param[out] row The next row produced by the scan
   * @param[out] rid The next row RID produced by the scan
   * @return `true` if a row was produced, `false` if there are no more rows
   */
  bool Next(Row *row, RowId *rid) override;

  /** @return The output schema for the sequential scan */
  const Schema *GetOutputSchema() const override { return plan_->OutputSchema(); }

    TableInfo* table_info;
    std::vector<RowId> RowSet;//result rid
    std::vector<RowId>::iterator itr;
    std::vector<AbstractExpression*> need_seq;//store the predicate which has not index

    void getPredicate(std::vector<single_predicate>& pred,AbstractExpression* exp);

 private:

  /** The sequential scan plan node to be executed */
  const IndexScanPlanNode *plan_;
};