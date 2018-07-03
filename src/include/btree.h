#ifndef BTREE_H__
#define BTREE_H__

#include <stdbool.h>

#include "cs165_api.h"

#define FANOUT 408

typedef struct BTreeNode {
  bool is_leaf;
  int vals[FANOUT];
  size_t idxs[FANOUT];
  size_t length;
  struct BTreeNode* children[FANOUT + 1];
  struct BTreeNode* next;
} BTreeNode;
// size ~= 20 * FANOUT + 25, e.g. 8192 ~ 408

BTreeNode* build_btree(int* vals, size_t* idxs, size_t length);
BTreeNode* btree_search(BTreeNode* node, int val);
void btree_insert(BTreeNode** node, int val, size_t val_pos, bool clustered);
void btree_delete(BTreeNode* node, size_t pos);
void link_btree_nodes(BTreeNode* node);
void print_btree(BTreeNode* node);
void free_btree(BTreeNode* node);

typedef enum ShiftDirection { LEFT, RIGHT } ShiftDirection;

#endif

/**
 *  type  |  size
 * ------ +  ----
 *  bool  |   1
 *  int   |   4
 * size_t |   8
 *  ptr*  |   8
 **/
