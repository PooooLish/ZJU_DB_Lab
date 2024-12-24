#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

#include <cstdio>
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
int yyparse(void);
// FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
    char path[] = "./databases";
    DIR *dir;
    if((dir = opendir(path)) == nullptr) {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }

    struct dirent *stdir;
    while((stdir = readdir(dir)) != nullptr) {
        if( strcmp( stdir->d_name , "." ) == 0 ||
            strcmp( stdir->d_name , "..") == 0 ||
            stdir->d_name[0] == '.')
            continue;
        dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    }

    closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
    switch (plan->GetType()) {
        // Create a new sequential scan executor
        case PlanType::SeqScan: {
            return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
        }
            // Create a new index scan executor
        case PlanType::IndexScan: {
            return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
        }
            // Create a new update executor
        case PlanType::Update: {
            auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
            return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
        }
            // Create a new delete executor
        case PlanType::Delete: {
            auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
            return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
        }
        case PlanType::Insert: {
            auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
            auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
            return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
        }
        case PlanType::Values: {
            return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
        }
        default:
            throw std::logic_error("Unsupported plan type.");
    }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx) {
    // Construct the executor for the abstract plan node
    auto executor = CreateExecutor(exec_ctx, plan);

    try {
        executor->Init();
        RowId rid{};
        Row row{};
        while (executor->Next(&row, &rid)) {
            if (result_set != nullptr) {
                result_set->push_back(row);
            }
        }
    } catch (const exception &ex) {
        std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
        if (result_set != nullptr) {
            result_set->clear();
        }
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
    if (ast == nullptr) {
        return DB_FAILED;
    }
    auto start_time = std::chrono::system_clock::now();
    unique_ptr<ExecuteContext> context(nullptr);
    if(!current_db_.empty())
        context = dbs_[current_db_]->MakeExecuteContext(nullptr);
    switch (ast->type_) {
        case kNodeCreateDB:
            return ExecuteCreateDatabase(ast, context.get());
        case kNodeDropDB:
            return ExecuteDropDatabase(ast, context.get());
        case kNodeShowDB:
            return ExecuteShowDatabases(ast, context.get());
        case kNodeUseDB:
            return ExecuteUseDatabase(ast, context.get());
        case kNodeShowTables:
            return ExecuteShowTables(ast, context.get());
        case kNodeCreateTable:
            return ExecuteCreateTable(ast, context.get());
        case kNodeDropTable:
            return ExecuteDropTable(ast, context.get());
        case kNodeShowIndexes:
            return ExecuteShowIndexes(ast, context.get());
        case kNodeCreateIndex:
            return ExecuteCreateIndex(ast, context.get());
        case kNodeDropIndex:
            return ExecuteDropIndex(ast, context.get());
        case kNodeTrxBegin:
            return ExecuteTrxBegin(ast, context.get());
        case kNodeTrxCommit:
            return ExecuteTrxCommit(ast, context.get());
        case kNodeTrxRollback:
            return ExecuteTrxRollback(ast, context.get());
        case kNodeExecFile:
            return ExecuteExecfile(ast, context.get());
        case kNodeQuit:
            return ExecuteQuit(ast, context.get());
        default:
            break;
    }
    // Plan the query.
    Planner planner(context.get());
    std::vector<Row> result_set{};
    try {
        planner.PlanQuery(ast);
        // Execute the query.
        ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
    } catch (const exception &ex) {
        std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
        return DB_FAILED;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
            double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    // Return the result set as string.
    std::stringstream ss;
    ResultWriter writer(ss);

    if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
        auto schema = planner.plan_->OutputSchema();
        auto num_of_columns = schema->GetColumnCount();
        if (!result_set.empty()) {
            // find the max width for each column
            vector<int> data_width(num_of_columns, 0);
            for (const auto &row : result_set) {
                for (uint32_t i = 0; i < num_of_columns; i++) {
                    data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
                }
            }
            int k = 0;
            for (const auto &column : schema->GetColumns()) {
                data_width[k] = max(data_width[k], int(column->GetName().length()));
                k++;
            }
            // Generate header for the result set.
            writer.Divider(data_width);
            k = 0;
            writer.BeginRow();
            for (const auto &column : schema->GetColumns()) {
                writer.WriteHeaderCell(column->GetName(), data_width[k++]);
            }
            writer.EndRow();
            writer.Divider(data_width);

            // Transforming result set into strings.
            for (const auto &row : result_set) {
                writer.BeginRow();
                for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
                    writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
                }
                writer.EndRow();
            }
            writer.Divider(data_width);
        }
        writer.EndInformation(result_set.size(), duration_time, true);
    } else {
        writer.EndInformation(result_set.size(), duration_time, false);
    }
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
    switch (result) {
        case DB_ALREADY_EXIST:
            cout << "Database already exists." << endl;
            break;
        case DB_NOT_EXIST:
            cout << "Database not exists." << endl;
            break;
        case DB_TABLE_ALREADY_EXIST:
            cout << "Table already exists." << endl;
            break;
        case DB_TABLE_NOT_EXIST:
            cout << "Table not exists." << endl;
            break;
        case DB_INDEX_ALREADY_EXIST:
            cout << "Index already exists." << endl;
            break;
        case DB_INDEX_NOT_FOUND:
            cout << "Index not exists." << endl;
            break;
        case DB_COLUMN_NAME_NOT_EXIST:
            cout << "Column not exists." << endl;
            break;
        case DB_KEY_NOT_FOUND:
            cout << "Key not exists." << endl;
            break;
        case DB_QUIT:
            cout << "Bye." << endl;
            break;
        default:
            break;
    }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
    if (ast->child_==nullptr) return DB_FAILED;

    string db_name(ast->child_->val_); // 获得数据库名字

    if (dbs_[db_name] != nullptr) {
        std::cout << "database " << db_name << " exists." << endl;
        return DB_FAILED;
    }

    DBStorageEngine* new_database = new DBStorageEngine(db_name, true);
    dbs_[db_name]=new_database;
    //->dbs_.insert(pair<std::string, DBStorageEngine*>(db_name, new_database));
/*
  auto size = this->dbs_.size();
  ofstream out_file;
  out_file.open("/mnt/d/database_name.txt");
  out_file << size << std::endl;
  for(auto itr : this->dbs_){
    out_file << itr.first << std::endl;
  }
  out_file.close();
*/
    std::cout << "Create database success" << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    if (ast->child_ == nullptr) return DB_FAILED;

    std::string db_name(ast->child_->val_);

    if (dbs_[db_name] == nullptr) {
        std::cout << "database " << db_name << " not exists." << endl;
        return DB_FAILED;
    }

    if (current_db_ == db_name) current_db_ = "";
    auto target_db = dbs_[db_name];
    target_db->~DBStorageEngine();
    dbs_.erase(db_name);
/*
  auto size = this->dbs_.size();
  ofstream out_file;
  out_file.open("/mnt/d/database_name.txt");
  out_file << size << std::endl;
  for(auto itr : this->dbs_){
    out_file << itr.first << std::endl;
  }
  out_file.close();
 */
    std::cout << "Drop database success" << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    std::cout << "Database(s) of number: " << dbs_.size() << endl;

    std::cout << "+-------------+" << std::endl;
    std::cout << "| Database    |" << std::endl;
    std::cout << "+-------------+" << std::endl;
    for(auto it : dbs_)
        std::cout << "| " << setw(12) << left << it.first << "|" << std::endl;
    std::cout << "+-------------+" << std::endl;

/*
  for (const auto &db : dbs_) {
    std::cout << db.first << std::endl;
  }
  */

    std::cout << "Show database success" << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    if (ast->child_ == nullptr) return DB_FAILED;
    std::string db_name(ast->child_->val_);
    if (dbs_[db_name] == nullptr) {
        std::cout << "database " << db_name << " not exists." << endl;
        return DB_FAILED;
    }
    current_db_ = db_name;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    DBStorageEngine *current_db_engine = dbs_[current_db_];
    if (current_db_engine == nullptr) {
        std::cout << "No database selected." << endl;
        return DB_FAILED;
    }
    std::vector<TableInfo *> table_info;
    current_db_engine->catalog_mgr_->GetTables(table_info);
    std::cout << "table(s) of number: " << table_info.size() << endl;
    for (auto table_info1 : table_info) {
        std::cout << table_info1->GetTableName() << std::endl;
    }
    std::cout << "Show table success" << std::endl;

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    if (dbs_[current_db_] == nullptr) {
        std::cout << "no database selected" << std::endl;
        return DB_FAILED;
    }
    pSyntaxNode ptr = ast->child_;
    string table_name(ptr->val_);
    ptr = ptr->next_->child_;
    std::vector<Column *> columns;
    std::vector<std::string> uni, pri;
    string column_name, column_type;
    TypeId type;
    uint32_t column_length = 4;
    for (uint32_t i = 0; ptr != nullptr; i++, ptr = ptr->next_) {
        if (ptr->type_ == kNodeColumnDefinition) {
            column_name = ptr->child_->val_;
            column_type = ptr->child_->next_->val_;
            if (column_type == "int") {
                type = kTypeInt;
            } else if (column_type == "float") {
                type = kTypeFloat;
            } else if (column_type == "char") {
                type = kTypeChar;
                column_length = atoi(ptr->child_->next_->child_->val_);
                if (column_length <= 0 || strchr(ptr->child_->next_->child_->val_, '.') != nullptr) {
                    std::cout << "char invalid" << std::endl;
                    return DB_FAILED;
                }
            } else {
                type = kTypeInvalid;
                cout << "type invalid" << std::endl;
                return DB_FAILED;
            }
            bool unique = ptr->val_ != nullptr;
            if (unique) uni.push_back(column_name);
            Column *column_ptr;
            if (type == kTypeInt || type == kTypeFloat)
                column_ptr = new Column(column_name, type, i, false, unique);
            else
                column_ptr = new Column(column_name, type, column_length, i, false, unique);
            columns.push_back(column_ptr);
        } else if (ptr->type_ == kNodeColumnList) {
            pSyntaxNode pri_key_node = ptr->child_;
            while (pri_key_node) {
                string pri_name(pri_key_node->val_);
                pri.push_back(pri_name);
                uni.push_back(pri_name);
                pri_key_node = pri_key_node->next_;
            }
        }
    }
    auto *schema = new Schema(columns);
    TableInfo *table_info = nullptr;
    auto mgr = dbs_[current_db_]->catalog_mgr_;
    bool create = mgr->CreateTable(table_name, schema, nullptr, table_info);
    if (!create) return DB_FAILED;
    //table_info->SetPrimaryKey(pri);
    //table_info->SetUniqueKey(uni);
    table_info->table_meta_->primary_key_name = pri;
    table_info->table_meta_->unique_key_name = uni;
    cout << "Create table success" << endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
    if (ast->child_ == nullptr) return DB_FAILED;
    string drop_table_name(ast->child_->val_);
    return dbs_[current_db_]->catalog_mgr_->DropTable(drop_table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
    auto cata_manager = dbs_[current_db_]->catalog_mgr_;
    std::vector<TableInfo *> table_infos;
    auto res = cata_manager->GetTables(table_infos);
    if (res != DB_SUCCESS) {
        return DB_FAILED;
    }
    for (auto &table_info : table_infos) {
        string table_name = table_info->GetTableName();
        std::vector<IndexInfo *> idx_list;
        res = cata_manager->GetTableIndexes(table_name, idx_list);
        if (res != DB_SUCCESS) {
            return res;
        }
        for (auto idx : idx_list) {
            std::cout << idx->GetIndexName() << std::endl;
        }
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    auto idx_node = ast->child_;
    auto cata_manager = dbs_[current_db_]->catalog_mgr_;
    string idx_name = idx_node->val_;
    auto table_node = idx_node->next_;
    string table_name = table_node->val_;
    auto keys_node = table_node->next_;

    TableInfo *table_info;
    auto res = cata_manager->GetTable(table_name, table_info);
    if (res != DB_SUCCESS) return DB_FAILED;
    auto uni = table_info->table_meta_->unique_key_name;
    std::vector<string> col_names;
    if (keys_node == nullptr)
        return DB_FAILED;
    else {
        for (keys_node = keys_node->child_; keys_node != nullptr; keys_node = keys_node->next_) {
            col_names.emplace_back(keys_node->val_);
        }
        bool flag = false;
        for (const auto &iter1 : uni) {
            for (const auto &iter2 : col_names) {
                if (iter1 == iter2) {
                    flag = true;
                    break;
                }
            }
        }
        if (!flag) {
            cout << "not unique" << endl;
            return DB_FAILED;
        }
    }
    IndexInfo *index_info;
    res = cata_manager->CreateIndex(table_name, idx_name, col_names, nullptr, index_info, "bptree");
    if (res != DB_SUCCESS) return DB_FAILED;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    auto idx_node = ast->child_;
    string idx_name = idx_node->val_;
    auto cata_manager = dbs_[current_db_]->catalog_mgr_;
    std::vector<TableInfo *> table_infos;
    auto res = cata_manager->GetTables(table_infos);
    if (res != DB_SUCCESS) {
        return DB_FAILED;
    }
    for (auto table_info : table_infos) {
        string table_name = table_info->GetTableName();
        std::vector<IndexInfo *> index_infos;
        res = cata_manager->GetTableIndexes(table_name, index_infos);
        if (res != DB_SUCCESS) {
            return DB_FAILED;
        }
        for (auto index_info : index_infos) {
            if (index_info->GetIndexName() == idx_name) {
                res = cata_manager->DropIndex(table_info->GetTableName(), idx_name);
                if (res != DB_SUCCESS) {
                    return res;
                }
            }
            return DB_SUCCESS;
        }
    }
}


dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
    std::ifstream ist;
    std::string file_name=ast->child_->val_;
    ist.open(file_name);
    const int maxsize=1024;
    char cmd[maxsize];
    // for print syntax tree
    // TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
    uint32_t syntax_tree_id = 0;
    clock_t Ts=clock();
    for(int i=1;ist.getline(cmd,maxsize);i++)
    {
        cout<<file_name<<" line "<<i<<": "<<cmd<<'\n';
        YY_BUFFER_STATE bp = yy_scan_string(cmd);
        if (bp == nullptr) {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);
        // init parser module
        MinisqlParserInit();
        // parse
        yyparse();
        // parse result handle
        if (MinisqlParserGetError()) {
            // error
            printf("%s\n", MinisqlParserGetErrorMessage());
        } else {
            // Comment them out if you don't need to debug the syntax tree
            printf("[INFO] Sql syntax parse ok!\n");
            // SyntaxTreePrinter printer(MinisqlGetParserRootNode());
            // printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
        }
        auto result = this->Execute(MinisqlGetParserRootNode());
        // clean memory after parse
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
    }
    clock_t Tt=clock();
    cout<<"Query OK, "<< "(" <<(double)(Tt-Ts)/CLOCKS_PER_SEC<<"sec)"<<endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    return DB_QUIT;
}
