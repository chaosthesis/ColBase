#ifndef HASH_TABLE
#define HASH_TABLE

#include "cs165_api.h"

#define SLOT 4

typedef struct HashNode {
  size_t key[SLOT];
  int val[SLOT];
  size_t size;
  struct HashNode* next;
} HashNode;
//size = 12 * SLOT + 16, e.g. 64 ~ 4

typedef struct HashTable {
  HashNode** buckets;
  size_t next_split;
  size_t round;
  size_t capacity;
} HashTable;

HashTable* create_hashtable(size_t size);
void print_hashtable(HashTable* ha_tbl);
void free_hashtable(HashTable* ha_tbl);
void hashtable_put(HashTable* ha_tbl, size_t key, int val);
void hashtable_get(HashTable* ha_tbl, size_t key, Result* res);

#endif
