#ifndef INDEX_H__
#define INDEX_H__

void init_sorted_index(Column* col, size_t* idxs);

void delete_index(Column* col, size_t pos);

void insert_index(Column* col, int val, size_t val_pos);

void rebuild_index(Column* col, size_t* idxs);

#endif