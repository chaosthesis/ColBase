#include <stdio.h>
#include <string.h>

#include "btree.h"
#include "client_context.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "utils.h"

void init_sorted_index(Column* col, size_t* idxs) {
  if (col->size > 0) {
    SortedIndex* payload = (SortedIndex*)(col->index.payload);
    int* vals = payload->vals;
    size_t* position = payload->pos;

    for (size_t i = 0; i < col->size; i++) {
      vals[i] = col->data[i];
      position[i] = idxs ? idxs[i] : i;
    }

    merge_sort(vals, position, 0, col->size - 1);
  }
}

/*=== INSERT INDEX ===*/

void insert_sorted_index(Column* col, int val, size_t val_pos) {
  if (!col->clustered) {
    SortedIndex* payload = (SortedIndex*)(col->index.payload);
    int* vals = payload->vals;
    size_t* position = payload->pos;

    size_t pos = pos_in_sorted(vals, col->size, val);
    array_insert(vals, col->size, val, pos);
    position[pos] = val_pos;

    for (size_t i = 0; i < col->size; i++)
      if (position[i] >= val_pos && i != pos) position[i]++;
  }
}

void insert_btree_index(Column* col, int val, size_t val_pos) {
  if (col->index.payload == NULL) {
    size_t natural_order[col->size];
    for (size_t i = 0; i < col->size; i++) natural_order[i] = i;
    build_btree(col->data, natural_order, col->size);
  }

  BTreeNode* node = (BTreeNode*)(col->index.payload);
  btree_insert(&node, val, val_pos, col->clustered);
  col->index.payload = node;
  // print_btree(node); printf("inserted %d at position %zu\n\n", val, val_pos);
}

void insert_index(Column* col, int val, size_t val_pos) {
  switch (col->index.type) {
    case NONE:
      break;
    case SORTED:
      insert_sorted_index(col, val, val_pos);
      break;
    case BTREE:
      insert_btree_index(col, val, val_pos);
      break;
  }
}

/*=== DELETE INDEX ===*/

void delete_sorted_index(Column* col, size_t position) {
  if (!col->clustered) {
    SortedIndex* payload = (SortedIndex*)(col->index.payload);

    array_delete(payload->vals, col->size, position);
    
    for (size_t i = 0; i < col->size; i++)
      if ((payload->pos)[i] >= position) (payload->pos)[i]--;
  }
}

void delete_btree_index(Column* col, size_t pos) {
  BTreeNode* node = (BTreeNode*)(col->index.payload);
  btree_delete(node, pos);
  // print_btree(node); printf(" deleted pos %zu\n\n", pos);
}

void delete_index(Column* col, size_t pos) {
  switch (col->index.type) {
    case NONE:
      break;
    case SORTED:
      delete_sorted_index(col, pos);
      break;
    case BTREE:
      delete_btree_index(col, pos);
      break;
  }
}

/*=== REBUILD INDEX ===*/

void rebuild_sorted_index(Column* col, size_t* idxs) {
  if (!col->clustered) init_sorted_index(col, idxs);
}

void rebuild_btree_index(Column* col, size_t* idxs) {
  free_btree((BTreeNode*)(col->index.payload));
  if (col->clustered) {
    col->index.payload = build_btree(col->data, idxs, col->size);
  } else {
    int* data_copy = malloc(sizeof(int) * col->size);
    memcpy(data_copy, col->data, sizeof(int) * col->size);
    merge_sort(data_copy, idxs, 0, col->size - 1);
    col->index.payload = build_btree(data_copy, idxs, col->size);
    free(data_copy);
  }
}

void rebuild_index(Column* col, size_t* idxs) {
  switch (col->index.type) {
    case NONE:
      break;
    case SORTED:
      rebuild_sorted_index(col, idxs);
      break;
    case BTREE:
      rebuild_btree_index(col, idxs);
      break;
  }
}
