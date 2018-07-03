#ifndef SELECT_H__
#define SELECT_H__

Result* single_select(Comparator* cmp);

void shared_select(BatchSelect* batch_pointer);

#endif
