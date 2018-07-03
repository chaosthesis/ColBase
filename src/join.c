#include <pthread.h>
#include <stdio.h>
#include <sys/time.h>

#include "cs165_api.h"
#include "db_manager.h"
#include "hash_table.h"
#include "join.h"
#include "utils.h"

struct timeval tm1, tm2;

/*=== Nested Loop Join ===*/

size_t nested_loop_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                        size_t size_l, size_t size_r, int* output_l,
                        int* output_r) {
  if (size_l <= size_r) {
    size_t k = 0;
    for (size_t i = 0; i < size_l; i++)
      for (size_t j = 0; j < size_r; j++)
        if (val_l[i] == val_r[j]) {
          output_l[k] = pos_l[i];
          output_r[k++] = pos_r[j];
        }
    return k;
  } else {
    return nested_loop_join(val_r, pos_r, val_l, pos_l, size_r, size_l,
                            output_r, output_l);
  }
}

size_t block_nested_loop_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                              size_t size_l, size_t size_r, int* output_l,
                              int* output_r) {
  if (size_l <= size_r) {
    size_t k = 0;
    size_t p = PAGE_SIZE / sizeof(int);
    for (size_t i = 0; i < size_l; i += p)
      for (size_t j = 0; j < size_r; j += p)
        for (size_t r = i; r < i + p && r < size_l; r++)
          for (size_t m = j; m < j + p && m < size_r; m++)
            if (val_l[r] == val_r[m]) {
              output_l[k] = pos_l[r];
              output_r[k++] = pos_r[m];
            }
    return k;
  } else {
    return block_nested_loop_join(val_r, pos_r, val_l, pos_l, size_r, size_l,
                                  output_r, output_l);
  }
}

/*=== Hash Join ===*/

size_t hash_join(int* val_l, int* pos_l, int* val_r, int* pos_r, size_t size_l,
                 size_t size_r, int* output_l, int* output_r) {
  if (size_l > size_r)
    return hash_join(val_r, pos_r, val_l, pos_l, size_r, size_l, output_r,
                     output_l);

  gettimeofday(&tm1, NULL);

  size_t res_capacity = size_l > size_r ? size_l : size_r;
  HashTable* ha_tbl = create_hashtable(res_capacity);

  for (size_t i = 0; i < size_l; i++) hashtable_put(ha_tbl, val_l[i], pos_l[i]);
  // print_hashtable(ha_tbl); printf("\n");

  gettimeofday(&tm2, NULL);
  printf("build hashtable >> %.3f ms\n\n",
         (double)(tm2.tv_usec - tm1.tv_usec) / 1000 +
             (double)(tm2.tv_sec - tm1.tv_sec) * 1000);

  gettimeofday(&tm1, NULL);

  size_t res_size = 0;
  Result* res = calloc(sizeof(Result), 1);
  res->payload = malloc(sizeof(int) * DEFAULT_CAPACITY);
  res->data_type = INT;

  for (size_t i = 0; i < size_r; i++) {
    hashtable_get(ha_tbl, val_r[i], res);
    for (size_t j = 0; j < res->num_tuples; j++) {
      output_l[res_size + j] = ((int*)res->payload)[j];
      output_r[res_size + j] = pos_r[i];
    }
    res_size += res->num_tuples;
  }
  free_result(res);

  gettimeofday(&tm2, NULL);
  printf("probe hashtable >> %.3f ms\n\n",
         (double)(tm2.tv_usec - tm1.tv_usec) / 1000 +
             (double)(tm2.tv_sec - tm1.tv_sec) * 1000);

  free_hashtable(ha_tbl);
  return res_size;
}

/*=== Parallel Hash Join ===*/

static size_t jobs;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t res_lock = PTHREAD_MUTEX_INITIALIZER;

void* hashjoin_task(void* args) {
  HashJoinArgs* arg = (HashJoinArgs*)args;
  pthread_mutex_lock(&lock);
  while (jobs) {
    size_t i = --jobs;
    pthread_mutex_unlock(&lock);

    Result* res = calloc(sizeof(Result), 1);
    res->payload = malloc(sizeof(int) * DEFAULT_CAPACITY);
    res->data_type = INT;

    hashtable_get(arg->ha_tbl, arg->val_r[i], res);

    pthread_mutex_lock(&res_lock);
    size_t offset = *(arg->offset);
    *(arg->offset) += res->num_tuples;
    pthread_mutex_unlock(&res_lock);

    for (size_t j = 0; j < res->num_tuples; j++) {
      arg->output_l[offset + j] = ((int*)res->payload)[j];
      arg->output_r[offset + j] = arg->pos_r[i];
    }
    free_result(res);
    pthread_mutex_lock(&lock);
  }
  pthread_mutex_unlock(&lock);
  return NULL;
}

size_t parallel_hash_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                          size_t size_l, size_t size_r, int* output_l,
                          int* output_r) {
  if (size_l > size_r)
    return parallel_hash_join(val_r, pos_r, val_l, pos_l, size_r, size_l,
                              output_r, output_l);

  gettimeofday(&tm1, NULL);

  size_t res_capacity = size_l > size_r ? size_l : size_r;
  HashTable* ha_tbl = create_hashtable(res_capacity);

  for (size_t i = 0; i < size_l; i++) hashtable_put(ha_tbl, val_l[i], pos_l[i]);
  // print_hashtable(ha_tbl); printf("\n");

  gettimeofday(&tm2, NULL);
  printf("build hashtable >> %.3f ms\n\n",
         (double)(tm2.tv_usec - tm1.tv_usec) / 1000 +
             (double)(tm2.tv_sec - tm1.tv_sec) * 1000);

  size_t res_size = 0;

  pthread_t thread[PROC_NUM];
  jobs = size_r;
  HashJoinArgs* args = malloc(sizeof(HashJoinArgs));
  args->val_r = val_r;
  args->pos_r = pos_r;
  args->ha_tbl = ha_tbl;
  args->output_l = output_l;
  args->output_r = output_r;
  args->offset = &res_size;

  gettimeofday(&tm1, NULL);

  for (size_t i = 0; i < PROC_NUM; i++)
    pthread_create(&thread[i], NULL, hashjoin_task, (void*)args);

  for (size_t i = 0; i < PROC_NUM; i++) pthread_join(thread[i], NULL);

  gettimeofday(&tm2, NULL);
  printf("probe hashtable >> %.3f ms\n\n",
         (double)(tm2.tv_usec - tm1.tv_usec) / 1000 +
             (double)(tm2.tv_sec - tm1.tv_sec) * 1000);

  free(args);
  free_hashtable(ha_tbl);

  return res_size;
}
