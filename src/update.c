#include <stdio.h>

#include "client_context.h"
#include "cs165_api.h"
#include "index.h"
#include "insert.h"
#include "utils.h"

void column_delete(Column* col, int* pos, size_t size) {
  for (size_t i = 0; i < size; i++) {
    array_delete(col->data, col->size, pos[i]);
    delete_index(col, pos[i]);
    col->size--;
  }
}

void delete_scheduler(Table* table, Result* pos_del) {
  int* pos = (int*)(pos_del->payload);
  size_t size = pos_del->num_tuples;

  for (size_t i = 0; i < table->col_count; i++)
    column_delete(table->columns + i, pos, size);

  table->size -= size;
}

void update_scheduler(Table* tbl, size_t col_idx, Result* pos, int val) {
  int vals[pos->num_tuples][tbl->col_count];

  for (size_t i = 0; i < pos->num_tuples; i++)
    for (size_t j = 0; j < tbl->col_count; j++)
      vals[i][j] =
          j == col_idx ? val : tbl->columns[j].data[((int*)pos->payload)[i]];

  delete_scheduler(tbl, pos);

  for (size_t i = 0; i < pos->num_tuples; i++)
    insert_scheduler(tbl, &vals[i][0]);
}
