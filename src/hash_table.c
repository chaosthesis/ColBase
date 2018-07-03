#include <stdio.h>
#include <stdlib.h>

#include "cs165_api.h"
#include "hash_table.h"
#include "utils.h"

size_t pow_2(size_t exp) {
  size_t res = 1;
  while (exp-- > 0) res *= 2;
  return res;
}

void print_bucket(HashNode* bucket) {
  while (bucket) {
    printf(" -> ");
    for (size_t i = 0; i < bucket->size; i++)
      printf("{%zu:%d} ", bucket->key[i], bucket->val[i]);
    bucket = bucket->next;
  }
}

void print_hashtable(HashTable* ha_tbl) {
  printf("next = %zu, round = %zu\n", ha_tbl->next_split, ha_tbl->round);
  size_t tail = pow_2(ha_tbl->round) + ha_tbl->next_split;
  printf("size = %zu\n", tail);
  for (size_t i = 0; i < tail; i++) {
    printf("%zu", i);
    print_bucket(ha_tbl->buckets[i]);
    printf("\n");
  }
}

size_t hash(HashTable* ha_tbl, size_t key) {
  size_t bkt_idx = key % pow_2(ha_tbl->round);
  if (bkt_idx < ha_tbl->next_split) bkt_idx = key % pow_2(ha_tbl->round + 1);
  return bkt_idx;
}

HashNode* create_bucket() {
  HashNode* bucket = calloc(sizeof(HashNode), 1);
  bucket->size = 0;
  return bucket;
}

void free_bucket(HashNode* bucket) {
  if (bucket->next) free_bucket(bucket->next);
  free(bucket);
}

HashTable* create_hashtable(size_t size) {
  HashTable* ha_tbl = malloc(sizeof(HashTable));
  ha_tbl->capacity = (size / SLOT) * 2;
  ha_tbl->buckets = malloc(sizeof(HashNode*) * ha_tbl->capacity);
  ha_tbl->buckets[0] = create_bucket();
  ha_tbl->next_split = 0;
  ha_tbl->round = 0;
  return ha_tbl;
}

void resize_hashtable(HashTable* ha_tbl, size_t new_capacity) {
  ha_tbl->buckets =
      realloc(ha_tbl->buckets, sizeof(HashNode*) * ha_tbl->capacity);
  ha_tbl->capacity = new_capacity;
}

void bucket_put(HashNode* bucket, size_t key, int val) {
  while (bucket->next) bucket = bucket->next;
  if (bucket->size < SLOT) {
    bucket->key[bucket->size] = key;
    bucket->val[bucket->size++] = val;
  } else {
    bucket->next = calloc(sizeof(HashNode), 1);
    bucket->next->key[0] = key;
    bucket->next->val[0] = val;
    bucket->next->size = 1;
  }
}

void hashtable_split(HashTable* ha_tbl) {
  size_t splitting = ha_tbl->next_split++;
  size_t tail = pow_2(ha_tbl->round) + splitting;

  if (tail >= ha_tbl->capacity) resize_hashtable(ha_tbl, ha_tbl->capacity * 2);

  HashNode* bucket = ha_tbl->buckets[splitting];
  ha_tbl->buckets[splitting] = create_bucket();
  ha_tbl->buckets[tail] = create_bucket();

  HashNode* bkt_cur = bucket;
  while (bkt_cur) {
    for (size_t i = 0; i < bkt_cur->size; i++) {
      size_t key = bkt_cur->key[i];
      int val = bkt_cur->val[i];
      size_t hash_val = hash(ha_tbl, key);
      bucket_put(ha_tbl->buckets[hash_val], key, val);
    }
    bkt_cur = bkt_cur->next;
  }
  free_bucket(bucket);

  if (ha_tbl->next_split == pow_2(ha_tbl->round)) {
    ha_tbl->round++;
    ha_tbl->next_split = 0;
  }
}

void hashtable_put(HashTable* ha_tbl, size_t key, int val) {
  size_t putting = hash(ha_tbl, key);
  HashNode* bucket = ha_tbl->buckets[putting];

  while (bucket->next) bucket = bucket->next;

  if (bucket->size < SLOT) {
    bucket_put(bucket, key, val);
  } else {
    hashtable_split(ha_tbl);
    putting = hash(ha_tbl, key);
    bucket = ha_tbl->buckets[putting];
    bucket_put(bucket, key, val);
  }
}

void hashtable_get(HashTable* ha_tbl, size_t key, Result* res) {
  res->num_tuples = 0;
  int* output = (int*)(res->payload);
  size_t getting = hash(ha_tbl, key);

  HashNode* bucket = ha_tbl->buckets[getting];
  while (bucket) {
    for (size_t i = 0; i < bucket->size; i++)
      if (key == bucket->key[i]) {
        if (res->num_tuples + 1 >= DEFAULT_CAPACITY)
            output = realloc(output, sizeof(int) * (res->num_tuples + 1));
        output[res->num_tuples++] = bucket->val[i];
      }

    bucket = bucket->next;
  }
}

void free_hashtable(HashTable* ha_tbl) {
  size_t tail = pow_2(ha_tbl->round) + ha_tbl->next_split;
  for (size_t i = 0; i < tail; i++) free_bucket(ha_tbl->buckets[i]);
  free(ha_tbl->buckets);
  free(ha_tbl);
}
