#ifndef JOIN_H__
#define JOIN_H__

#include "hash_table.h"

typedef struct HashJoinArgs {
  HashTable* ha_tbl;
  int* val_r;
  int* pos_r;
  int* output_l;
  int* output_r;
  size_t* offset;
} HashJoinArgs;

size_t nested_loop_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                        size_t size_l, size_t size_r, int* output_l,
                        int* output_r);

size_t block_nested_loop_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                              size_t size_l, size_t size_r, int* output_l,
                              int* output_r);

size_t hash_join(int* val_l, int* pos_l, int* val_r, int* pos_r, size_t size_l,
                 size_t size_r, int* output_l, int* output_r);

size_t parallel_hash_join(int* val_l, int* pos_l, int* val_r, int* pos_r,
                          size_t size_l, size_t size_r, int* output_l,
                          int* output_r);

#endif