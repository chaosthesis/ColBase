#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "btree.h"
#include "cs165_api.h"
#include "utils.h"

/*=== Select ===*/

Result* select_from_sorted(Comparator* cmp) {
  Column* col = cmp->gen_col->column_pointer.column;

  int* input = NULL;
  size_t* pos = NULL;

  if (col->clustered) {
    input = col->data;
  } else {
    SortedIndex* payload = (SortedIndex*)(col->index.payload);
    input = payload->vals;
    pos = payload->pos;
  }

  size_t pos_low = binary_search(input, col->size, cmp->p_low);
  size_t pos_high = binary_search(input, col->size, cmp->p_high);
  if (cmp->p_low > input[pos_low]) pos_low++;
  if (cmp->p_high <= input[pos_high]) pos_high--;

  size_t res_size = pos_high - pos_low + 1;
  int* output = malloc(sizeof(int) * res_size);

  if (col->clustered) {
    for (size_t i = 0; i < res_size; i++) output[i] = i + pos_low;
  } else {
    for (size_t i = 0; i < res_size; i++)
      output[i] = pos[i + pos_low];
  }

  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = res_size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

Result* select_from_btree(Comparator* cmp) {
  Column* col = cmp->gen_col->column_pointer.column;
  BTreeNode* root = (BTreeNode*)(col->index.payload);

  int p_low = cmp->p_low;
  int p_high = cmp->p_high;

  BTreeNode* node_low = btree_search(root, p_low);
  BTreeNode* node_high = btree_search(root, p_high);
  BTreeNode* cur = node_low;

  size_t res_size = 0;
  size_t res_capacity = DEFAULT_CAPACITY;
  int* output = malloc(sizeof(int) * res_capacity);

  while (cur) {
    for (size_t i = 0; i < cur->length; i++) {
      if (cur->vals[i] < p_low) {
        continue;
      } else if (cur->vals[i] >= p_high) {
        break;
      }
      else {
        if (res_size >= res_capacity) resize_array(&output, &res_capacity);
        output[res_size++] = cur->idxs[i];
      }
    }
    if (cur == node_high) break;
    cur = cur->next;
  }

  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = res_size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

Result* select_from_column(Comparator* cmp) {
  Column* col = cmp->gen_col->column_pointer.column;
  switch (col->index.type) {
    case NONE:
      break;
    case SORTED:
      return select_from_sorted(cmp);
    case BTREE:
      return select_from_btree(cmp);
  }

  int* input = col->data;
  size_t input_size = col->size;

  int p_low = cmp->p_low;
  int p_high = cmp->p_high;
  int cmp_type = cmp->type1 + cmp->type2;

  size_t res_size = 0;
  size_t res_capacity = DEFAULT_CAPACITY;
  int* output = malloc(sizeof(int) * res_capacity);

  switch (cmp_type) {
    case LESS_THAN: {
      for (size_t i = 0; i < input_size; i++) {
        if (res_size >= res_capacity) resize_array(&output, &res_capacity);
        if (input[i] < p_high) output[res_size++] = i;
      }
      break;
    }
    case GREATER_THAN_OR_EQUAL: {
      for (size_t i = 0; i < input_size; i++) {
        if (res_size >= res_capacity) resize_array(&output, &res_capacity);
        if (input[i] >= p_low) output[res_size++] = i;
      }
      break;
    }
    case RANGE: {
      for (size_t i = 0; i < input_size; i++) {
        if (res_size >= res_capacity) resize_array(&output, &res_capacity);
        if (input[i] >= p_low && input[i] < p_high) output[res_size++] = i;
      }
      break;
    }
    default:
      return NULL;
  }

  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = res_size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

Result* select_from_result(Comparator* cmp) {
  size_t input_id_size = cmp->gen_col_id->column_pointer.result->num_tuples;
  size_t input_size = cmp->gen_col->column_pointer.result->num_tuples;
  if (input_size != input_id_size) return NULL;

  int p_low = cmp->p_low;
  int p_high = cmp->p_high;
  int cmp_type = cmp->type1 + cmp->type2;

  int* input_id = (int*)cmp->gen_col_id->column_pointer.result->payload;
  int* input = (int*)cmp->gen_col->column_pointer.result->payload;
  int* output = malloc(sizeof(int) * input_id_size);
  size_t res_size = 0;

  switch (cmp_type) {
    case LESS_THAN: {
      for (size_t i = 0; i < input_id_size; i++) {
        if (input[i] < p_high) output[res_size++] = input_id[i];
      }
      break;
    }
    case GREATER_THAN_OR_EQUAL: {
      for (size_t i = 0; i < input_id_size; i++) {
        if (input[i] >= p_low) output[res_size++] = input_id[i];
      }
      break;
    }
    case RANGE: {
      for (size_t i = 0; i < input_id_size; i++) {
        if (input[i] >= p_low && input[i] < p_high)
          output[res_size++] = input_id[i];
      }
      break;
    }
    default:
      return NULL;
  }

  Result* result = calloc(sizeof(Result), 1);
  result->num_tuples = res_size;
  result->data_type = INT;
  result->payload = output;
  return result;
}

Result* single_select(Comparator* cmp) {
  Result* res = NULL;
  switch (cmp->gen_col->column_type) {
    case COLUMN: {
      res = select_from_column(cmp);
      break;
    }
    case RESULT: {
      res = select_from_result(cmp);
      break;
    }
    default:
      break;
  }
  free(cmp->gen_col);
  if (cmp->gen_col_id) free(cmp->gen_col_id);
  free(cmp);
  return res;
}

/*=== Shared Scan ===*/

static size_t jobs;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

void* select_task(void* args) {
  BatchSelect* batch = (BatchSelect*)args;
  pthread_mutex_lock(&lock);
  while (jobs) {
    size_t idx = --jobs;
    pthread_mutex_unlock(&lock);
    batch->results[idx] = single_select(batch->comparators[idx]);
    pthread_mutex_lock(&lock);
  }
  pthread_mutex_unlock(&lock);
  return NULL;
}

void shared_select(BatchSelect* batch) {
  pthread_t thread[PROC_NUM];
  jobs = batch->size;

  for (size_t i = 0; i < PROC_NUM; i++)
    pthread_create(&thread[i], NULL, select_task, (void*)batch);

  for (size_t i = 0; i < PROC_NUM; i++) pthread_join(thread[i], NULL);
}
