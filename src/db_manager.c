#define _BSD_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "btree.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "utils.h"

Db* current_db;

static char db_path[PATH_SIZE];
static char db_meta_path[PATH_SIZE];
static char tbl_path[PATH_SIZE];
static char tbl_meta_path[PATH_SIZE];
static char col_path[PATH_SIZE];
static char col_meta_path[PATH_SIZE];
static char col_data_path[PATH_SIZE];
static char idx_path[PATH_SIZE];
static char idx_data_path[PATH_SIZE];

bool not_current_db(char* db_name) {
  return (strcmp(current_db->name, db_name) != 0);
}

/*=== CREATE DB OBJECTS ===*/

void create_column(char* name, Table* tbl) {
  Column* col = tbl->columns + tbl->col_ready;
  strcpy(col->name, name);

  sprintf(col_path, "%s/%s/%s/%s", DATA_DIR, current_db->name, tbl->name,
          col->name);
  mkdir(col_path, 0777);

  sprintf(col_data_path, "%s/col_data", col_path);
  col->data_fd = open(col_data_path, O_CREAT | O_RDWR, S_IRWXU);
  if (col->data_fd == -1) perror("Error open()");

  size_t length = sizeof(int) * TABLE_CAPACITY;
  ftruncate(col->data_fd, (off_t)length);
  col->data =
      mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED, col->data_fd, 0);

  col->size = 0;
  col->index.type = NONE;
  col->clustered = false;
  tbl->col_ready++;
}

void create_table(Db* db, const char* name, size_t num_columns) {
  sprintf(tbl_path, "%s/%s/%s", DATA_DIR, current_db->name, name);
  mkdir(tbl_path, 0777);

  for (size_t i = 0; i < db->size; i++) {
    if (strcmp(db->tables[i].name, name) == 0) {
      log_err("Table already exists.");
      return;
    }
  }

  resize_db_if_full(db);
  Table* new_tbl = db->tables + db->size;
  memset(new_tbl, 0, sizeof(Table));
  strcpy(new_tbl->name, name);
  new_tbl->col_count = num_columns;
  new_tbl->col_ready = 0;
  new_tbl->size = 0;
  new_tbl->capacity = TABLE_CAPACITY;
  new_tbl->columns = calloc(sizeof(Column), num_columns);
  db->size++;
}

void add_db(const char* db_name) {
  sprintf(db_path, "%s/%s", DATA_DIR, db_name);
  mkdir(DATA_DIR, 0777);
  mkdir(db_path, 0777);

  if (current_db) {
    sync_db(current_db);
    free(current_db);
  }
  current_db = calloc(sizeof(Db), 1);
  strcpy(current_db->name, db_name);
  current_db->size = 0;
  current_db->capacity = DEFAULT_CAPACITY;
  current_db->tables = calloc(sizeof(Table), DEFAULT_CAPACITY);
}

/*=== SYNC DB OBJECTS ===*/

void sync_sorted_idx(Column* col) {
  if (col->clustered) return;
  SortedIndex* payload = (SortedIndex*)(col->index.payload);

  size_t length;

  length = sizeof(int) * col->size;
  ftruncate(payload->vals_fd, (off_t)length);
  msync(payload->vals, length, MS_SYNC);
  munmap(payload->vals, length);
  close(payload->vals_fd);

  length = sizeof(size_t) * col->size;
  ftruncate(payload->pos_fd, (off_t)length);
  msync(payload->pos, length, MS_SYNC);
  munmap(payload->pos, length);
  close(payload->pos_fd);
}

void sync_btree_idx(BTreeNode* node, FILE* fp) {
  fwrite(node, sizeof(BTreeNode), 1, fp);
  fwrite(node->vals, sizeof(int), node->length, fp);
  if (node->is_leaf) {
    fwrite(node->idxs, sizeof(size_t), node->length, fp);
  } else {
    for (size_t i = 0; i <= node->length; i++)
      sync_btree_idx(node->children[i], fp);
  }
}

void sync_column(Column* col, char* table_path) {
  sprintf(col_path, "%s/%s", table_path, col->name);
  sprintf(idx_path, "%s/idx", col_path);

  // save column meta data
  sprintf(col_meta_path, "%s/col_meta", col_path);
  FILE* fp = fopen(col_meta_path, "wb");
  fwrite(col, sizeof(Column), 1, fp);
  fclose(fp);

  // save column values
  size_t length = sizeof(int) * col->size;
  ftruncate(col->data_fd, (off_t)length);
  msync(col->data, length, MS_SYNC);
  munmap(col->data, length);
  close(col->data_fd);

  // save column index
  switch (col->index.type) {
    case NONE:
      break;
    case SORTED:
      sync_sorted_idx(col);
      break;
    case BTREE: {
      sprintf(idx_data_path, "%s/b_tree", idx_path);
      fp = fopen(idx_data_path, "wb");
      sync_btree_idx((BTreeNode*)(col->index.payload), fp);
      fclose(fp);
      break;
    }
  }
}

void sync_table(Table* tbl, char* db_path) {
  sprintf(tbl_path, "%s/%s", db_path, tbl->name);
  sprintf(tbl_meta_path, "%s/tbl_meta", tbl_path);

  FILE* fp = fopen(tbl_meta_path, "wb");
  fwrite(tbl, sizeof(Table), 1, fp);

  for (size_t i = 0; i < tbl->col_count; i++) {
    Column* col = tbl->columns + i;
    fwrite(col->name, 1, NAME_SIZE, fp);
    sync_column(col, tbl_path);
  }

  fclose(fp);
}

void sync_db(Db* db) {
  sprintf(db_path, "%s/%s", DATA_DIR, db->name);
  sprintf(db_meta_path, "%s/db_meta", db_path);

  FILE* fp = fopen(db_meta_path, "wb");
  fwrite(db, sizeof(Db), 1, fp);

  for (size_t i = 0; i < db->size; i++) {
    Table* tbl = db->tables + i;
    fwrite(tbl->name, 1, NAME_SIZE, fp);
    sync_table(tbl, db_path);
  }
  fclose(fp);
}

/*=== LOAD DB OBJECTS ===*/

void load_sorted_idx(Column* col, int vals_fd, int pos_fd) {
  SortedIndex* sorted_index = malloc(sizeof(SortedIndex));

  struct stat sb;

  sorted_index->vals_fd = vals_fd;
  fstat(vals_fd, &sb);
  sorted_index->vals =
      mmap(0, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, vals_fd, 0);

  sorted_index->pos_fd = pos_fd;
  fstat(pos_fd, &sb);
  sorted_index->pos =
      mmap(0, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, pos_fd, 0);

  col->index.payload = sorted_index;
}

BTreeNode* load_btree_node(FILE* fp) {
  BTreeNode* node = malloc(sizeof(BTreeNode));
  fread(node, sizeof(BTreeNode), 1, fp);
  fread(node->vals, sizeof(int), node->length, fp);
  if (node->is_leaf) {
    fread(node->idxs, sizeof(size_t), node->length, fp);
  } else {
    for (size_t i = 0; i <= node->length; i++)
      node->children[i] = load_btree_node(fp);
  }
  return node;
}

void load_btree_idx(Column* col, FILE* fp) {
  BTreeNode* root = load_btree_node(fp);
  link_btree_nodes(root);
  col->index.payload = root;
}

void load_column(Column* col, char* col_name, char* table_path) {
  sprintf(col_path, "%s/%s", table_path, col_name);
  sprintf(col_meta_path, "%s/col_meta", col_path);
  sprintf(col_data_path, "%s/col_data", col_path);
  sprintf(idx_path, "%s/idx", col_path);
  sprintf(idx_data_path, "%s/b_tree", idx_path);

  FILE* fp = fopen(col_meta_path, "rb");
  fread(col, sizeof(Column), 1, fp);
  fclose(fp);

  col->data_fd = open(col_data_path, O_RDWR);
  struct stat sb;
  fstat(col->data_fd, &sb);
  col->data =
      mmap(0, sb.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, col->data_fd, 0);

  switch (col->index.type) {
    case NONE:
      break;
    case SORTED: {
      if (!col->clustered) {
        sprintf(idx_data_path, "%s/sorted_vals", idx_path);
        int vals_fd = open(idx_data_path, O_RDWR, S_IRWXU);

        sprintf(idx_data_path, "%s/sorted_pos", idx_path);
        int pos_fd = open(idx_data_path, O_RDWR, S_IRWXU);

        load_sorted_idx(col, vals_fd, pos_fd);
      }
      break;
    }
    case BTREE: {
      fp = fopen(idx_data_path, "rb");
      load_btree_idx(col, fp);
      fclose(fp);
      break;
    }
  }
}

void load_table(Table* table, char* tbl_name, char* db_path) {
  sprintf(tbl_path, "%s/%s", db_path, tbl_name);
  sprintf(tbl_meta_path, "%s/tbl_meta", tbl_path);

  // load table
  FILE* fp = fopen(tbl_meta_path, "rb");
  fread(table, sizeof(Table), 1, fp);

  // load table's columns
  table->columns = malloc(sizeof(Column) * table->col_count);
  char col_name[NAME_SIZE];
  for (size_t i = 0; i < table->col_count; i++) {
    fread(col_name, 1, NAME_SIZE, fp);
    load_column(table->columns + i, col_name, tbl_path);
  }
  fclose(fp);
}

Db* load_db(char* db_name) {
  sprintf(db_path, "%s/%s", DATA_DIR, db_name);
  sprintf(db_meta_path, "%s/db_meta", db_path);

  // load db
  Db* db = malloc(sizeof(Db));
  FILE* fp = fopen(db_meta_path, "rb");
  fread(db, sizeof(Db), 1, fp);

  // load db's tables
  db->tables = malloc(sizeof(Table) * db->capacity);
  char tbl_name[NAME_SIZE];
  for (size_t i = 0; i < db->size; i++) {
    fread(tbl_name, 1, NAME_SIZE, fp);
    load_table(db->tables + i, tbl_name, db_path);
  }

  fclose(fp);
  return db;
}

/*=== CLEANING UTILS ===*/

void free_column(Column* col) {
  if (col) {
    switch (col->index.type) {
      case NONE:
        break;
      case SORTED:
        if (!col->clustered) free(col->index.payload);
        break;
      case BTREE:
        free_btree((BTreeNode*)(col->index.payload));
        break;
    }
  }
}

void free_table(Table* table) {
  if (table) {
    for (size_t i = 0; i < table->col_count; i++)
      free_column(table->columns + i);
    free(table->columns);
  }
}

void free_db(Db* db) {
  if (db) {
    for (size_t i = 0; i < db->size; i++) free_table(db->tables + i);
    free(db->tables);
    free(db);
  }
}

void free_result(Result* result) {
  if (result) {
    if (result->payload) free(result->payload);
    free(result);
  }
}

void free_batch(BatchSelect* batch) {
  free(batch->comparators);
  free(batch->results);
  free(batch->handles);
  free(batch);
}

void free_context(ClientContext* context) {
  free_batch(context->batched_queries);
  for (int i = 0; i < context->chandles_in_use; i++) {
    GeneralizedColumn gen_col = context->chandle_table[i].generalized_column;
    if (gen_col.column_type == RESULT)
      free_result(gen_col.column_pointer.result);
  }
  free(context->chandle_table);
}

void free_operator(DbOperator* query) {
  if (query) {
    switch (query->type) {
      case INSERT: {
        InsertOperator op = query->operator_fields.insert_operator;
        free(op.values);
        break;
      }
      case AVG:
      case SUM: {
        AvgSumOperator op = query->operator_fields.avg_sum_operators;
        free(op.gen_col);
        break;
      }
      case PRINT: {
        PrintOperator op = query->operator_fields.print_operator;
        for (size_t i = 0; i < op.handle_num; i++) free(op.handles[i]);
        free(op.handles);
        break;
      }
      default:
        break;
    }
    free(query);
  }
}

void shutdown_database(Db* db) {
  sync_db(db);
  free_db(db);
}
