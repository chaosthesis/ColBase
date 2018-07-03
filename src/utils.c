#define _BSD_SOURCE

#include "utils.h"
#include <ctype.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "btree.h"
#include "cs165_api.h"

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
//#define LOG_INFO 1

/*=== RESIZING UTILS ===*/

void resize_array(int** array, size_t* capacity) {
  *array = realloc(*array, sizeof(int) * (*capacity) * 2);
  *capacity *= 2;
}

void resize_db_if_full(Db* db) {
  if (db->size >= db->capacity) {
    Table* tables = db->tables;
    tables = realloc(tables, sizeof(Table) * db->capacity * 2);
    db->tables = tables;
    db->capacity *= 2;
  }
}

void* resize_mmap(void* addr, int fd, size_t new_len) {
  struct stat sb;
  fstat(fd, &sb);
  msync(addr, sb.st_size, MS_SYNC);
  munmap(addr, sb.st_size);
  ftruncate(fd, (off_t)new_len);
  return mmap(NULL, new_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

void resize_sorted_index(Column* col, size_t new_size) {
  SortedIndex* payload = (SortedIndex*)(col->index.payload);
  payload->vals =
      resize_mmap(payload->vals, payload->vals_fd, sizeof(int) * new_size);
  payload->pos =
      resize_mmap(payload->pos, payload->pos_fd, sizeof(size_t) * new_size);
}

void resize_table(Table* tbl) {
  size_t new_capacity = tbl->capacity * 2;
  for (size_t i = 0; i < tbl->col_count; i++) {
    Column* col = tbl->columns + i;
    col->data =
        resize_mmap(col->data, col->data_fd, sizeof(int) * new_capacity);
    if (col->index.type == SORTED && !col->clustered)
      resize_sorted_index(col, new_capacity);
  }
  tbl->capacity = new_capacity;
}

void resize_context_if_full(ClientContext* context) {
  if (context->chandles_in_use >= context->chandle_slots) {
    context->chandle_slots *= 2;
    context->chandle_table =
        realloc(context->chandle_table,
                sizeof(GeneralizedColumnHandle) * context->chandle_slots);
  }
}

void resize_batch_if_full(BatchSelect* batch) {
  if (batch->size >= batch->capacity) {
    batch->capacity *= 2;
    batch->comparators =
        realloc(batch->comparators, sizeof(Comparator*) * batch->capacity);
    batch->results = realloc(batch->results, sizeof(Result*) * batch->capacity);
    batch->handles =
        realloc(batch->handles, sizeof(char[NAME_SIZE]) * batch->capacity);
  }
}

void resize_buffer(char** buffer, size_t* buffer_capacity) {
  *buffer = realloc(*buffer, (*buffer_capacity) * 2);
  *buffer_capacity *= 2;
}

/*=== ARRAY UTILS ===*/

size_t binary_search(int* vals, size_t length, int val) {
  if (length == 0) return 0;
  size_t low = 0;
  size_t high = length - 1;
  size_t mid = (low + high) / 2;
  while (low < high) {
    if (vals[mid] < val) {
      low = mid + 1;
    } else if (vals[mid] == val) {
      return mid;
    } else {
      high = mid > 0 ? mid - 1 : mid;
    }
    mid = (low + high) / 2;
  }
  while (low > 0 && vals[low - 1] == val) low--;
  return low;
}

size_t pos_in_sorted(int* vals, size_t length, int new_val) {
  if (length == 0) return 0;
  size_t pos = binary_search(vals, length, new_val);
  if (new_val > vals[pos]) pos++;
  return pos;
}

void array_insert(int* vals, size_t length, int new_val, size_t pos) {
  if (length > pos) memmove(vals + pos + 1, vals + pos, (length - pos));
  vals[pos] = new_val;
}

void array_delete(int* vals, size_t length, size_t pos) {
  if (length > pos + 1) memmove(vals + pos, vals + pos + 1, (length - pos - 1));
}

void array_reorder(int* vals, size_t* pos, size_t length) {
  int* tmp = malloc(sizeof(int) * length);
  for (size_t i = 0; i < length; i++) tmp[i] = vals[pos[i]];
  for (size_t i = 0; i < length; i++) vals[i] = tmp[i];
  free(tmp);
}

void merge(int* vals, size_t* idxs, size_t low, size_t mid, size_t high) {
  int* vals_tmp = malloc(sizeof(int) * (high - low + 1));
  size_t* idxs_tmp = malloc(sizeof(size_t) * (high - low + 1));

  size_t cur_l = low;
  size_t cur_r = mid + 1;
  size_t cur;

  for (cur = 0; cur_l <= mid && cur_r <= high; cur++) {
    if (vals[cur_l] <= vals[cur_r]) {
      vals_tmp[cur] = vals[cur_l];
      idxs_tmp[cur] = idxs[cur_l++];
    } else {
      vals_tmp[cur] = vals[cur_r];
      idxs_tmp[cur] = idxs[cur_r++];
    }
  }
  while (cur_l <= mid) {
    vals_tmp[cur] = vals[cur_l];
    idxs_tmp[cur++] = idxs[cur_l++];
  }
  while (cur_r <= high) {
    vals_tmp[cur] = vals[cur_r];
    idxs_tmp[cur++] = idxs[cur_r++];
  }

  for (cur = low; cur <= high; cur++) {
    vals[cur] = vals_tmp[cur - low];
    idxs[cur] = idxs_tmp[cur - low];
  }

  free(vals_tmp);
  free(idxs_tmp);
}

void merge_sort(int* vals, size_t* idxs, size_t low, size_t high) {
  if (low < high) {
    size_t mid = (low + high) / 2;
    merge_sort(vals, idxs, low, mid);
    merge_sort(vals, idxs, mid + 1, high);
    merge(vals, idxs, low, mid, high);
  }
}

/*=== STRING UTILS ===*/

char* trim_whitespace(char* str) {
  int length = strlen(str);
  int current = 0;
  
  for (int i = 0; i < length; ++i)
    if (!isspace(str[i])) str[current++] = str[i];
  
  str[current] = '\0';
  return str;
}

char* trim_parenthesis(char* str) {
  int length = strlen(str);
  int current = 0;
  
  for (int i = 0; i < length; ++i)
    if (!(str[i] == '(' || str[i] == ')')) str[current++] = str[i];
  
  str[current] = '\0';
  return str;
}

char* trim_quotes(char* str) {
  int length = strlen(str);
  int current = 0;
  
  for (int i = 0; i < length; ++i)
    if (str[i] != '\"') str[current++] = str[i];
  
  str[current] = '\0';
  return str;
}

/*=== LOGGING UTILS ===*/

/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char* format, ...) {
#ifdef LOG
  va_list v;
  va_start(v, format);
  vfprintf(out, format, v);
  va_end(v);
#else
  (void)out;
  (void)format;
#endif
}

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char* format, ...) {
#ifdef LOG_ERR
  va_list v;
  va_start(v, format);
  fprintf(stderr, ANSI_COLOR_RED);
  vfprintf(stderr, format, v);
  fprintf(stderr, ANSI_COLOR_RESET);
  va_end(v);
#else
  (void)format;
#endif
}

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char* format, ...) {
#ifdef LOG_INFO
  va_list v;
  va_start(v, format);
  fprintf(stdout, ANSI_COLOR_GREEN);
  vfprintf(stdout, format, v);
  fprintf(stdout, ANSI_COLOR_RESET);
  fflush(stdout);
  va_end(v);
#else
  (void)format;
#endif
}
