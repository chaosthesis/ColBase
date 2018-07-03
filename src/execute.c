#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "insert.h"
#include "join.h"
#include "select.h"
#include "update.h"
#include "utils.h"

struct timeval tv1, tv2;

/*=== SELECT ===*/

void select_scheduler(DbOperator* query) {
  ClientContext* context = query->context;
  SelectOperator op = query->operator_fields.select_operator;
  BatchSelect* batch = context->batched_queries;

  if (batch->is_collecting) {
    resize_batch_if_full(batch);
    batch->comparators[batch->size] = op.comparator;
    strcpy(batch->handles[batch->size++], op.handle);
  } else {
    Result* result = single_select(op.comparator);
    update_context(context, op.handle, result);
  }
}

/*=== FETCH ===*/

Result* fetch(Column* col, Result* ids) {
  size_t size = ids->num_tuples;
  int* input_ids = (int*)ids->payload;
  int* input_vals = col->data;
  int* output = malloc(sizeof(int) * size);

  for (size_t i = 0; i < size; i++) output[i] = input_vals[input_ids[i]];

  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

/*=== AGGREGATE ===*/

Result* avg_sum(GeneralizedColumn* gen_col, OperatorType op) {
  int* input = NULL;
  size_t input_size = 0;
  switch (gen_col->column_type) {
    case RESULT: {
      input = (int*)gen_col->column_pointer.result->payload;
      input_size = gen_col->column_pointer.result->num_tuples;
      break;
    }
    case COLUMN: {
      input = gen_col->column_pointer.column->data;
      input_size = gen_col->column_pointer.column->size;
    }
  }

  Result* res = calloc(sizeof(Result), 1);
  if (input_size == 0) {
    res->num_tuples = 0;
    return res;
  }
  res->num_tuples = 1;

  long sum = 0;
  for (size_t i = 0; i < input_size; i++) sum += (long)input[i];

  switch (op) {
    case AVG: {
      double* output = malloc(sizeof(double));
      *output = (double)sum / input_size;
      res->data_type = DOUBLE;
      res->payload = output;
      break;
    }
    case SUM: {
      long* output = malloc(sizeof(long));
      *output = sum;
      res->data_type = LONG;
      res->payload = output;
    }
    default:
      break;
  }
  return res;
}

Result* add_sub(Result* val_l, Result* val_r, OperatorType op) {
  if (val_l->num_tuples != val_r->num_tuples) {
    log_err("Column sizes don't match.");
    return NULL;
  }
  size_t size = val_l->num_tuples;
  int* output = malloc(sizeof(int) * size);
  int* input1 = (int*)val_l->payload;
  int* input2 = (int*)val_r->payload;
  switch (op) {
    case ADD: {
      for (size_t i = 0; i < size; i++) output[i] = input1[i] + input2[i];
      break;
    }
    case SUB: {
      for (size_t i = 0; i < size; i++) output[i] = input1[i] - input2[i];
    }
    default:
      break;
  }
  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

Result* max_min(Result* vals_res, OperatorType op) {
  size_t size = vals_res->num_tuples;
  Result* res = calloc(sizeof(Result), 1);
  if (size == 0) {
    res->num_tuples = 0;
    return res;
  }
  int* input = (int*)vals_res->payload;
  int* output = malloc(sizeof(int));
  *output = input[0];
  switch (op) {
    case MAX: {
      for (size_t i = 0; i < size; i++)
        *output = *output < input[i] ? input[i] : *output;
      break;
    }
    case MIN: {
      for (size_t i = 0; i < size; i++)
        *output = *output > input[i] ? input[i] : *output;
    }
    default:
      break;
  }
  res->num_tuples = 1;
  res->data_type = INT;
  res->payload = output;
  return res;
}

/*=== JOIN ===*/

void join(Result* val_res_l, Result* pos_res_l, Result* val_res_r,
          Result* pos_res_r, JoinType join_type, Result** res_l,
          Result** res_r) {
  int* val_l = (int*)(val_res_l->payload);
  int* pos_l = (int*)(pos_res_l->payload);
  int* val_r = (int*)(val_res_r->payload);
  int* pos_r = (int*)(pos_res_r->payload);

  if (val_res_l->num_tuples != pos_res_l->num_tuples ||
      val_res_r->num_tuples != pos_res_r->num_tuples) {
    log_err("Join Failed: val and pos length don't match");
    return;
  }
  size_t size_l = val_res_l->num_tuples;
  size_t size_r = val_res_r->num_tuples;
  size_t res_capacity = size_l > size_r ? size_l : size_r;

  int* output_l = malloc(sizeof(int) * res_capacity);
  int* output_r = malloc(sizeof(int) * res_capacity);

  size_t res_size = 0;
  if (join_type == NESTED) {
    res_size = nested_loop_join(val_l, pos_l, val_r, pos_r, size_l, size_r,
                                output_l, output_r);
  } else if (join_type == HASH) {
    res_size = hash_join(val_l, pos_l, val_r, pos_r, size_l, size_r, output_l,
                         output_r);
  }

  *res_l = calloc(sizeof(Result), 1);
  (*res_l)->num_tuples = res_size;
  (*res_l)->data_type = INT;
  (*res_l)->payload = output_l;

  *res_r = calloc(sizeof(Result), 1);
  (*res_r)->num_tuples = res_size;
  (*res_r)->data_type = INT;
  (*res_r)->payload = output_r;
}

/*=== PRINT ===*/

char* print(DbOperator* query) {
  ClientContext* context = query->context;
  char** handles = query->operator_fields.print_operator.handles;
  size_t handle_num = query->operator_fields.print_operator.handle_num;

  char* buffer = malloc(BUFFER_CAPACITY);
  size_t buf_capacity = BUFFER_CAPACITY;
  size_t buf_len = 0;
  buffer[0] = '\0';

  Result** res = malloc(sizeof(Result*) * handle_num);
  for (size_t i = 0; i < handle_num; i++)
    res[i] = lookup_handle_result(context, handles[i]);

  size_t num_tuples = res[0]->num_tuples;

  for (size_t i = 0; i < num_tuples; i++) {
    for (size_t j = 0; j < handle_num; j++) {
      if (buf_len + BUFFER_CAPACITY > buf_capacity)
        resize_buffer(&buffer, &buf_capacity);
      switch (res[j]->data_type) {
        case INT: {
          int* payload = (int*)(res[j]->payload);
          buf_len += sprintf(buffer + buf_len, "%d,", payload[i]);
          break;
        }
        case LONG: {
          long* payload = (long*)(res[j]->payload);
          buf_len += sprintf(buffer + strlen(buffer), "%ld,", *payload);
          break;
        }
        case DOUBLE: {
          double* payload = (double*)(res[j]->payload);
          buf_len += sprintf(buffer + strlen(buffer), "%.2f,", *payload);
        }
      }
      if (j >= handle_num - 1) buffer[buf_len - 1] = '\n';
    }
  }

  free(res);

  if (buf_len == 0) {
    free(buffer);
    return "";
  } else {
    buffer[buf_len - 1] = '\0';
    return buffer;
  }
}

/**
 *  execute_DbOperator takes as input the DbOperator and executes the query.
 */
char* execute_DbOperator(DbOperator* query) {
  char* buffer = "";
  if (query) {
    gettimeofday(&tv1, NULL);
    Result* result;
    switch (query->type) {
      case CREATE:
        break;
      case INSERT: {
        InsertOperator op = query->operator_fields.insert_operator;
        insert_scheduler(op.table, op.values);
        break;
      }
      case LOAD: {
        receive_and_load(query->client_fd);
        break;
      }
      case UPDATE: {
        UpdateOperator op = query->operator_fields.update_operator;
        update_scheduler(op.tbl, op.col_idx, op.pos, op.update_val);
        break;
      }
      case DELETE: {
        DeleteOperator op = query->operator_fields.delete_operator;
        delete_scheduler(op.table, op.pos);
        break;
      }
      case SELECT: {
        select_scheduler(query);
        break;
      }
      case FETCH: {
        FetchOperator op = query->operator_fields.fetch_operator;
        result = fetch(op.vals_col, op.ids_res);
        update_context(query->context, op.handle, result);
        break;
      }
      case JOIN: {
        Result* res_l;
        Result* res_r;
        JoinOperator op = query->operator_fields.join_operator;
        join(op.val_l, op.pos_l, op.val_r, op.pos_r, op.join_type, &res_l,
             &res_r);
        update_context(query->context, op.handle_l, res_l);
        update_context(query->context, op.handle_r, res_r);
        break;
      }
      case AVG:
      case SUM: {
        AvgSumOperator op = query->operator_fields.avg_sum_operators;
        result = avg_sum(op.gen_col, query->type);
        update_context(query->context, op.handle, result);
        break;
      }
      case ADD:
      case SUB: {
        AddSubOperator op = query->operator_fields.add_sub_operators;
        result = add_sub(op.val_l, op.val_r, query->type);
        update_context(query->context, op.handle, result);
        break;
      }
      case MAX:
      case MIN: {
        MaxMinOperator op = query->operator_fields.max_min_operators;
        result = max_min(op.vals_res, query->type);
        update_context(query->context, op.handle, result);
        break;
      }
      case PRINT: {
        buffer = print(query);
        break;
      }
      case CLOSE: {
        shutdown_database(current_db);
        break;
      }
    }
    free_operator(query);
  }

  gettimeofday(&tv2, NULL);
  printf(">> %.3f ms\n\n", (double)(tv2.tv_usec - tv1.tv_usec) / 1000 +
                               (double)(tv2.tv_sec - tv1.tv_sec) * 1000);

  return buffer;
}
