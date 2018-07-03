#define _XOPEN_SOURCE
#define _BSD_SOURCE
#include <stdio.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "index.h"
#include "utils.h"

/*=== INSERT ===*/

void column_insert(Column* col, int val, size_t pos) {
  array_insert(col->data, col->size, val, pos);
  insert_index(col, val, pos);
  col->size++;
}

void clustered_insert(Table* tbl, size_t clustered, int* vals) {
  size_t pos =
      pos_in_sorted(tbl->columns[clustered].data, tbl->size, vals[clustered]);
  for (size_t i = 0; i < tbl->col_count; i++)
    column_insert(tbl->columns + i, vals[i], pos);
}

void unclustered_insert(Table* tbl, int* vals) {
  for (size_t i = 0; i < tbl->col_count; i++)
    column_insert(tbl->columns + i, vals[i], tbl->size);
}

void insert_scheduler(Table* tbl, int* vals) {
  if (tbl->capacity <= tbl->size) resize_table(tbl);

  size_t clustered = lookup_primary_column(tbl);
  if (clustered < tbl->col_count) {
    clustered_insert(tbl, clustered, vals);
  } else {
    unclustered_insert(tbl, vals);
  }

  tbl->size++;
}

/*=== LOAD ===*/

void column_load(Column* col, int* vals, size_t* cluster_order, size_t size) {
  memmove(col->data + col->size, vals, sizeof(int) * size);
  col->size += size;

  if (col->clustered) {
    merge_sort(col->data, cluster_order, 0, col->size - 1);
  } else if (cluster_order) {
    array_reorder(col->data, cluster_order, col->size);
  }

  size_t* natural_order = malloc(sizeof(size_t) * col->size);
  for (size_t i = 0; i < col->size; i++) natural_order[i] = i;

  rebuild_index(col, natural_order);
  
  free(natural_order);
}

void clustered_load(Table* tbl, size_t clustered, int** vals, size_t size) {
  size_t* cluster_order = malloc(sizeof(size_t) * (tbl->size + size));
  for (size_t i = 0; i < tbl->size + size; i++) cluster_order[i] = i;

  column_load(tbl->columns + clustered, vals[clustered], cluster_order, size);

  for (size_t i = 0; i < tbl->col_count; i++)
    if (i != clustered)
      column_load(tbl->columns + i, vals[i], cluster_order, size);

  free(cluster_order);
}

void unclustered_load(Table* tbl, int** vals, size_t size) {
  for (size_t i = 0; i < tbl->col_count; i++)
    column_load(tbl->columns + i, vals[i], NULL, size);
}

void load_scheduler(Table* table, int** vals, size_t size) {
  while (table->capacity <= table->size + size) resize_table(table);

  size_t clustered = lookup_primary_column(table);

  if (clustered < table->col_count) {
    clustered_load(table, clustered, vals, size);
  } else {
    unclustered_load(table, vals, size);
  }

  table->size += size;
}

void receive_and_load(int client_fd) {
  FILE* fp = fdopen(dup(client_fd), "r");

  int line_size = 3 * NAME_SIZE;
  char* line = malloc(line_size);
  char* line_ = line;
  fgets(line, line_size, fp);
  char* db_name = strsep(&line, ".");

  if (current_db == NULL || not_current_db(db_name))
    current_db = load_db(db_name);

  char* tbl_name = strsep(&line, ".");
  Table* tbl = lookup_table(tbl_name);
  if (tbl == NULL) {
    log_err("Failed to load table.");
    return;
  }

  char* token;
  line = line_;
  size_t size = 0;
  size_t capacity = tbl->capacity;

  int** vals = malloc(sizeof(int*) * tbl->col_count);
  for (size_t i = 0; i < tbl->col_count; i++)
    vals[i] = malloc(sizeof(int) * capacity);

  while (fgets(line, line_size, fp) && strcmp(line, "EOF\n") != 0) {
    if (capacity <= size) {
      for (size_t i = 0; i < tbl->col_count; i++)
        vals[i] = realloc(vals[i], sizeof(int) * 2 * capacity);
      capacity *= 2;
    }
    size_t col_id = 0;
    while ((token = strsep(&line, ","))) vals[col_id++][size] = atoi(token);
    line = line_;
    if (col_id != tbl->col_count) log_err("Failed to load columns into table.");
    size++;
  }

  free(line_);
  fclose(fp);

  load_scheduler(tbl, vals, size);

  for (size_t i = 0; i < tbl->col_count; i++) free(vals[i]);
  free(vals);
}
