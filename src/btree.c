#include <limits.h>
#include <stdio.h>
#include <string.h>

#include "btree.h"
#include "cs165_api.h"
#include "utils.h"

/**
 * Copy output to this link to see btree graph:
 * http://ysangkok.github.io/js-clrs-btree/btree.html
 **/
void print_btree(BTreeNode* node) {
  printf("{\"keys\":[");
  for (size_t i = 0; i < node->length; i++) {
    printf("%d", node->vals[i]);
    if (i < node->length - 1) printf(",");
  }
  printf("]");
  if (node->is_leaf == false) {
    printf(",\"children\":[");
    for (size_t i = 0; i <= node->length; i++) {
      print_btree(node->children[i]);
      if (i < node->length) printf(",");
    }
    printf("]");
  }
  printf("}");
}

/*=== Node Utils ===*/

bool btree_node_full(BTreeNode* node) { return node->length >= FANOUT; }

bool btree_node_empty(BTreeNode* node) { return node->length <= 0; }

BTreeNode* create_btree_node(bool is_leaf) {
  BTreeNode* node = calloc(sizeof(BTreeNode), 1);
  node->is_leaf = is_leaf;
  return node;
}

void btree_node_move(BTreeNode* dst, size_t d_pos, BTreeNode* src, size_t s_pos,
                     size_t length) {
  if (length > 0) {
    memmove(dst->vals + d_pos, src->vals + s_pos, sizeof(int) * length);
    if (src->is_leaf) {
      memmove(dst->idxs + d_pos, src->idxs + s_pos, sizeof(size_t) * length);
    } else {
      memmove(dst->children + d_pos, src->children + s_pos,
              sizeof(BTreeNode*) * (length + 1));
    }
  }
}

void btree_node_shift(BTreeNode* node, size_t i, ShiftDirection direction) {
  switch (direction) {
    case RIGHT:
      btree_node_move(node, i + 1, node, i, node->length++ - i);
      break;
    case LEFT:
      btree_node_move(node, i - 1, node, i, node->length-- - i);
      break;
  }
}

void btree_split_child(BTreeNode* node, size_t i) {
  BTreeNode* child = node->children[i];
  BTreeNode* sibling = create_btree_node(child->is_leaf);

  sibling->length = child->length / 2;
  child->length = child->length - sibling->length;

  btree_node_move(sibling, 0, child, child->length, sibling->length);

  sibling->next = child->next;
  child->next = sibling;

  btree_node_shift(node, i, RIGHT);
  node->children[i + 1] = sibling;

  if (child->is_leaf) {
    node->vals[i] = sibling->vals[0];
  } else {
    child->length--;
    node->vals[i] = child->vals[child->length];
  }
}

BTreeNode* btree_split_root(BTreeNode* old_root) {
  BTreeNode* root = create_btree_node(false);
  root->children[0] = old_root;
  btree_split_child(root, 0);
  root->vals[0] = root->children[1]->vals[0];
  return root;
}

BTreeNode* btree_search(BTreeNode* node, int val) {
  while (node && node->is_leaf == false) {
    size_t pos = binary_search(node->vals, node->length, val);
    if (val >= node->vals[pos]) pos++;
    return btree_search(node->children[pos], val);
  }
  return node;
}

static BTreeNode** prev_next_ptr = NULL;

void link_btree_nodes(BTreeNode* node) {
  BTreeNode* first_child = node->children[0];
  if (!first_child->is_leaf) {
    for (size_t i = 0; i <= node->length; i++)
      link_btree_nodes(node->children[i]);
  } else {
    if (prev_next_ptr) *prev_next_ptr = first_child;

    for (size_t i = 0; i < node->length; i++)
      node->children[i]->next = node->children[i + 1];

    BTreeNode* last_child = node->children[node->length];
    prev_next_ptr = &(last_child->next);
  }
}

/*=== Initialize ===*/

void btree_append_leaf(BTreeNode** node_ptr, BTreeNode* leaf) {
  if (btree_node_full(*node_ptr)) *node_ptr = btree_split_root(*node_ptr);

  BTreeNode* node = *node_ptr;
  BTreeNode* last_child = node->children[node->length];

  if (!last_child->is_leaf) {
    if (btree_node_full(last_child)) {
      btree_split_child(node, node->length);
      last_child = node->children[node->length];
    }
    btree_append_leaf(&last_child, leaf);
  } else {
    node->vals[node->length] = leaf->vals[0];
    node->children[node->length + 1] = leaf;
    node->length++;
  }
}

BTreeNode* build_btree(int* vals, size_t* idxs, size_t length) {
  BTreeNode* root = create_btree_node(false);

  size_t num_leaf = length / FANOUT;
  if (length % FANOUT) num_leaf++;

  BTreeNode* leaf[num_leaf];

  for (size_t i = 0; i < num_leaf; i++) {
    leaf[i] = create_btree_node(true);
    leaf[i]->length = i < num_leaf - 1 ? FANOUT : length % FANOUT;

    if (leaf[i]->length == 0) leaf[i]->length = FANOUT;

    memmove(leaf[i]->vals, vals + FANOUT * i, sizeof(int) * leaf[i]->length);
    memmove(leaf[i]->idxs, idxs + FANOUT * i, sizeof(size_t) * leaf[i]->length);
  }

  for (size_t i = 0; i < num_leaf - 1; i++) leaf[i]->next = leaf[i + 1];

  root->length = 1;
  root->children[0] = leaf[0];
  root->children[1] = num_leaf > 1 ? leaf[1] : NULL;
  root->vals[0] = num_leaf > 1 ? leaf[1]->vals[0] : INT_MAX;

  for (size_t i = 2; i < num_leaf; i++) btree_append_leaf(&root, leaf[i]);

  // print_btree(root); printf("\n\n");
  return root;
}

/*=== Update ===*/

void btree_update_val_idx(BTreeNode* leaf, int val, size_t pos,
                          bool clustered) {
  if (clustered) {
    size_t start = 0;
    while (leaf->idxs[start] < pos && start < leaf->length) start++;

    for (size_t i = start + 1; i < leaf->length; i++) leaf->idxs[i]++;

    while ((leaf = leaf->next)) {
      for (size_t i = 0; i < leaf->length; i++) leaf->idxs[i]++;
    }
  } else {
    bool touched_new = false;
    while (leaf) {
      for (size_t i = 0; i < leaf->length; i++) {
        if (leaf->idxs[i] >= pos) {
          if (leaf->vals[i] == val && !touched_new) {
            touched_new = true;
          } else {
            leaf->idxs[i]++;
          }
        }
      }
      leaf = leaf->next;
    }
  }
}

void btree_insert_not_full(BTreeNode* node, int val, size_t val_pos,
                           bool clustered) {
  size_t pos = binary_search(node->vals, node->length, val);
  if (node->length > 0 && val >= node->vals[pos]) pos++;
  if (node->is_leaf) {
    btree_node_shift(node, pos, RIGHT);
    node->vals[pos] = val;
    node->idxs[pos] = val_pos;
    btree_update_val_idx(node, val, val_pos, clustered);
  } else {
    BTreeNode* child = node->children[pos];
    if (btree_node_full(child)) {
      btree_split_child(node, pos);
      if (val >= node->vals[pos]) child = node->children[pos + 1];
    }
    btree_insert_not_full(child, val, val_pos, clustered);
  }
}

void btree_insert(BTreeNode** node, int val, size_t val_pos, bool clustered) {
  if (btree_node_full(*node)) {
    *node = btree_split_root(*node);
    size_t pos = val >= (*node)->vals[0] ? 1 : 0;
    btree_insert_not_full((*node)->children[pos], val, val_pos, clustered);
  } else {
    btree_insert_not_full(*node, val, val_pos, clustered);
  }
  // print_btree(*node); printf(" added %d\n\n", val);
}

void btree_delete(BTreeNode* node, size_t pos) {
  if (node->is_leaf) {
    while (node) {
      for (size_t i = 0; i < node->length; i++) {
        if (node->idxs[i] == pos) {
          btree_node_shift(node, i-- + 1, LEFT);
        } else if (node->idxs[i] > pos) {
          node->idxs[i]--;
        }
      }
      node = node->next;
    }
  } else {
    BTreeNode* child = node->children[0];
    while (btree_node_empty(child)) {
      btree_node_shift(node, 1, LEFT);
      child = node->children[0];
    }
    btree_delete(child, pos);
  }
}

/*=== Free ===*/

void free_btree(BTreeNode* node) {
  if (node) {
    if (!node->is_leaf)
      for (size_t i = 0; i <= node->length; i++) free_btree(node->children[i]);
    free(node);
  }
}
