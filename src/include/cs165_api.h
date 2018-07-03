/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#define PROC_NUM 16     // sysconf(_SC_NPROCESSORS_ONLN)
#define PAGE_SIZE 4096  // sysconf(_SC_PAGESIZE)
#define CACHE_SIZE 32768
#define CACHE_LINE_SIZE 64

#define DATA_DIR "../data"
#define NAME_SIZE 64
#define PATH_SIZE 256
#define DEFAULT_CAPACITY 32
#define TABLE_CAPACITY 256
#define BUFFER_CAPACITY 256

typedef enum IndexType { NONE, SORTED, BTREE } IndexType;

typedef struct SortedIndex {
  int* vals;
  size_t* pos;
  int vals_fd;
  int pos_fd;
} SortedIndex;

typedef struct ColumnIndex {
  IndexType type;
  void* payload;
} ColumnIndex;

typedef struct Column {
  char name[NAME_SIZE];
  int* data;
  int data_fd;
  size_t size;
  bool clustered;
  ColumnIndex index;
} Column;

typedef struct Table {
  char name[NAME_SIZE];
  Column* columns;
  size_t col_count;
  size_t col_ready;
  size_t size;
  size_t capacity;
} Table;

typedef struct Db {
  char name[NAME_SIZE];
  Table* tables;
  size_t size;
  size_t capacity;
} Db;

typedef enum StatusCode {
  OK,
  ERROR,
} StatusCode;

typedef struct Status {
  StatusCode code;
  char* error_message;
} Status;

typedef enum ComparatorType {
  NO_COMPARISON = 0,
  LESS_THAN = 1,
  GREATER_THAN = 2,
  EQUAL = 4,
  LESS_THAN_OR_EQUAL = 5,
  GREATER_THAN_OR_EQUAL = 6,
  RANGE = 7
} ComparatorType;

typedef enum DataType { INT, LONG, DOUBLE } DataType;

typedef struct Result {
  void* payload;
  size_t num_tuples;
  DataType data_type;
} Result;

typedef enum GeneralizedColumnType { RESULT, COLUMN } GeneralizedColumnType;

typedef union GeneralizedColumnPointer {
  Result* result;
  Column* column;
} GeneralizedColumnPointer;

typedef struct GeneralizedColumn {
  GeneralizedColumnType column_type;
  GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

typedef struct GeneralizedColumnHandle {
  char name[NAME_SIZE];
  GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;

typedef struct Comparator {
  long int p_low;
  long int p_high;
  GeneralizedColumn* gen_col;
  GeneralizedColumn* gen_col_id;
  ComparatorType type1;
  ComparatorType type2;
} Comparator;

typedef enum OperatorType {
  CREATE,
  INSERT,
  LOAD,
  UPDATE,
  DELETE,
  SELECT,
  FETCH,
  JOIN,
  AVG,
  SUM,
  ADD,
  SUB,
  MAX,
  MIN,
  PRINT,
  CLOSE
} OperatorType;

typedef struct InsertOperator {
  Table* table;
  int* values;
} InsertOperator;

typedef struct UpdateOperator {
  Table* tbl;
  size_t col_idx;
  Result* pos;
  int update_val;
} UpdateOperator;

typedef struct DeleteOperator {
  Table* table;
  Result* pos;
} DeleteOperator;

typedef struct SelectOperator {
  Comparator* comparator;
  char handle[NAME_SIZE];
} SelectOperator;

typedef struct FetchOperator {
  Column* vals_col;
  Result* ids_res;
  char handle[NAME_SIZE];
} FetchOperator;

typedef enum JoinType { NESTED, HASH } JoinType;

typedef struct JoinOperator {
  Result* val_l;
  Result* pos_l;
  Result* val_r;
  Result* pos_r;
  JoinType join_type;
  char handle_l[NAME_SIZE];
  char handle_r[NAME_SIZE];
} JoinOperator;

typedef struct AvgSumOperator {
  GeneralizedColumn* gen_col;
  char handle[NAME_SIZE];
} AvgSumOperator;

typedef struct AddSubOperator {
  Result* val_l;
  Result* val_r;
  char handle[NAME_SIZE];
} AddSubOperator;

typedef struct MaxMinOperator {
  Result* vals_res;
  char handle[NAME_SIZE];
} MaxMinOperator;

typedef struct PrintOperator {
  char** handles;
  size_t handle_num;
} PrintOperator;

typedef union OperatorFields {
  InsertOperator insert_operator;
  UpdateOperator update_operator;
  DeleteOperator delete_operator;
  SelectOperator select_operator;
  FetchOperator fetch_operator;
  JoinOperator join_operator;
  AvgSumOperator avg_sum_operators;
  AddSubOperator add_sub_operators;
  MaxMinOperator max_min_operators;
  PrintOperator print_operator;
} OperatorFields;

typedef struct BatchSelect {
  bool is_collecting;
  size_t size;
  size_t capacity;
  Result** results;
  Comparator** comparators;
  char (*handles)[NAME_SIZE];
} BatchSelect;

typedef struct ClientContext {
  BatchSelect* batched_queries;
  GeneralizedColumnHandle* chandle_table;
  int chandles_in_use;
  int chandle_slots;
} ClientContext;

typedef struct DbOperator {
  OperatorType type;
  OperatorFields operator_fields;
  int client_fd;
  ClientContext* context;
} DbOperator;

extern Db* current_db;

#endif /* CS165_H */
