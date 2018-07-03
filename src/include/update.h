#ifndef UPDATE_H
#define UPDATE_H

void update_scheduler(Table* tbl, size_t col_idx, Result* pos, int val);
void delete_scheduler(Table* table, Result* pos);

#endif