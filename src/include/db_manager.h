#ifndef DB_MANAGER_H__
#define DB_MANAGER_H__

#include "btree.h"

bool not_current_db(char* db_name);

void sync_db(Db* db);
Db* load_db(char* db_name);
void add_db(const char* db_name);

void create_table(Db* db, const char* name, size_t num_columns);
void create_column(char* name, Table* table);

/**
 * cleaning utilities
 **/
void free_db(Db* db);
void free_result(Result* result);
void free_batch(BatchSelect* batch);
void free_context(ClientContext* context);
void free_operator(DbOperator* query);
void free_btree(BTreeNode* node);

void shutdown_database(Db* db);

#endif
