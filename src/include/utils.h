// utils.h
// CS165 Fall 2017
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdarg.h>
#include "cs165_api.h"

/**
 * resizing utilities
 **/
void resize_array(int** array, size_t* capacity);
void resize_db_if_full(Db* db);
void resize_table(Table* tbl);
void resize_context_if_full(ClientContext* context);
void resize_batch_if_full(BatchSelect* batch_pointer);
void resize_buffer(char** buffer, size_t* buffer_capacity);

/**
 * array utilities
 **/
size_t binary_search(int* vals, size_t length, int n);
size_t pos_in_sorted(int* vals, size_t length, int new_val);
void array_insert(int* vals, size_t length, int new_val, size_t pos);
void array_delete(int* vals, size_t length, size_t pos);
void array_reorder(int* vals, size_t* pos, size_t length);
void quick_sort(int* vals, size_t low, size_t high, size_t* idxs);
void merge_sort(int* vals, size_t* idxs, size_t low, size_t high);

/**
 * string utilities
 **/
char* trim_parenthesis(char* str);
char* trim_whitespace(char* str);
char* trim_quotes(char* str);

/**
 * log utilities
 **/
void cs165_log(FILE* out, const char* format, ...);
void log_err(const char* format, ...);
void log_info(const char* format, ...);

#endif /* __UTILS_H__ */
