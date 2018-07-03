#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

ClientContext* init_context();
BatchSelect* init_batch();
void update_context(ClientContext* context, char* name, Result* data);

Table* lookup_table(char* name);
Column* lookup_column(char* tbl_name, char* name);
size_t lookup_primary_column(Table* tbl);
size_t lookup_column_idx(Table* tbl, char* col_name);
Result* lookup_handle_result(ClientContext* context, char* name);

#endif
