#include <string.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "utils.h"

BatchSelect* init_batch() {
  BatchSelect* batch = calloc(sizeof(BatchSelect), 1);
  batch->size = 0;
  batch->capacity = DEFAULT_CAPACITY;
  batch->is_collecting = false;
  batch->comparators = calloc(sizeof(Comparator*), DEFAULT_CAPACITY);
  batch->results = calloc(sizeof(Result*), DEFAULT_CAPACITY);
  batch->handles = calloc(sizeof(char[NAME_SIZE]), DEFAULT_CAPACITY);
  return batch;
}

ClientContext* init_context() {
  ClientContext* context = calloc(sizeof(ClientContext), 1);
  context->batched_queries = init_batch();
  context->chandle_table =
      calloc(sizeof(GeneralizedColumnHandle), DEFAULT_CAPACITY);
  context->chandles_in_use = 0;
  context->chandle_slots = DEFAULT_CAPACITY;
  return context;
}

Table* lookup_table(char* name) {
  for (size_t i = 0; i < current_db->size; i++) {
    if (strcmp(current_db->tables[i].name, name) == 0)
      return current_db->tables + i;
  }
  return NULL;
}

Column* lookup_column(char* tbl_name, char* name) {
  Table* tbl = lookup_table(tbl_name);
  for (size_t i = 0; i < tbl->col_count; i++) {
    if (strcmp(tbl->columns[i].name, name) == 0) return tbl->columns + i;
  }
  return NULL;
}

size_t lookup_primary_column(Table* tbl) {
  for (size_t i = 0; i < tbl->col_count; i++) {
    if (tbl->columns[i].clustered) return i;
  }
  return tbl->col_count;
}

size_t lookup_column_idx(Table* tbl, char* col_name) {
  for (size_t i = 0; i < tbl->col_count; i++) {
    if (strcmp(tbl->columns[i].name, col_name) == 0) return i;
  }
  return tbl->col_count;
}

int lookup_handle_id(ClientContext* context, char* name) {
  if (context && context->chandles_in_use > 0)
    for (int id = 0; id < context->chandles_in_use; id++) {
      if (strcmp(context->chandle_table[id].name, name) == 0) return id;
    }
  return -1;
}

Result* lookup_handle_result(ClientContext* context, char* name) {
  int id = lookup_handle_id(context, name);
  if (id == -1) return NULL;
  return context->chandle_table[id].generalized_column.column_pointer.result;
}

void update_context(ClientContext* context, char* name, Result* result) {
  int handle_id = lookup_handle_id(context, name);
  if (handle_id == -1) {
    resize_context_if_full(context);
    handle_id = context->chandles_in_use++;
    context->chandle_table[handle_id].generalized_column.column_type = RESULT;
  } else {
    free_result(context->chandle_table[handle_id]
                    .generalized_column.column_pointer.result);
  }

  strcpy(context->chandle_table[handle_id].name, name);
  context->chandle_table[handle_id].generalized_column.column_pointer.result =
      result;
}
