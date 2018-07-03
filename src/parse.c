#define _BSD_SOURCE

#include <ctype.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "client_context.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "index.h"
#include "message.h"
#include "parse.h"
#include "select.h"
#include "utils.h"

void parse_create_db(char* create_arguments) {
  char* token;
  token = strsep(&create_arguments, ",");
  if (token == NULL) return;

  char* db_name = token;
  db_name = trim_quotes(db_name);
  int last_char = strlen(db_name) - 1;
  if (last_char < 0 || db_name[last_char] != ')') return;
  db_name[last_char] = '\0';

  add_db(db_name);
}

void parse_create_tbl(char* create_arguments) {
  char** create_arguments_index = &create_arguments;
  char* table_name = strsep(create_arguments_index, ",");
  char* db_name = strsep(create_arguments_index, ",");
  char* col_cnt = strsep(create_arguments_index, ",");

  if (current_db == NULL || not_current_db(db_name))
    current_db = load_db(db_name);

  table_name = trim_quotes(table_name);
  col_cnt[strlen(col_cnt) - 1] = '\0';
  int column_cnt = atoi(col_cnt);

  create_table(current_db, table_name, column_cnt);
}

void parse_create_col(char* create_arguments) {
  char** create_arguments_index = &create_arguments;
  char* col_name = strsep(create_arguments_index, ",");
  char* db_name = strsep(create_arguments_index, ".");
  char* tbl_name = strsep(create_arguments_index, ",");

  if (current_db == NULL || not_current_db(db_name))
    current_db = load_db(db_name);

  tbl_name[strlen(tbl_name) - 1] = '\0';
  Table* tbl = lookup_table(tbl_name);
  if (tbl == NULL) {
    log_err("Cannot find the table");
    return;
  }
  col_name = trim_quotes(col_name);

  create_column(col_name, tbl);
}

void parse_create_idx(char* create_arguments) {
  char** create_arguments_index = &create_arguments;
  char* db_name = strsep(create_arguments_index, ".");
  char* tbl_name = strsep(create_arguments_index, ".");
  char* col_name = strsep(create_arguments_index, ",");
  char* idx_type = strsep(create_arguments_index, ",");
  char* cluster_type = strsep(create_arguments_index, ")");

  if (current_db == NULL || not_current_db(db_name))
    current_db = load_db(db_name);

  Column* col = lookup_column(tbl_name, col_name);
  if (col == NULL) {
    log_err("Cannot find the column");
    return;
  }

  col->clustered = (strcmp(cluster_type, "clustered") == 0) ? true : false;
  col->index.type = (strcmp(idx_type, "sorted") == 0) ? SORTED : BTREE;

  char idx_path[PATH_SIZE];
  sprintf(idx_path, "%s/%s/%s/%s/idx", DATA_DIR, db_name, tbl_name, col_name);
  mkdir(idx_path, 0777);

  if (!col->clustered && col->index.type == SORTED) {
    SortedIndex* sorted_index = malloc(sizeof(SortedIndex));

    char idx_data_path[PATH_SIZE];
    size_t length;

    sprintf(idx_data_path, "%s/sorted_vals", idx_path);
    sorted_index->vals_fd = open(idx_data_path, O_CREAT | O_RDWR, S_IRWXU);
    length = sizeof(int) * col->size;
    ftruncate(sorted_index->vals_fd, (off_t)length);
    sorted_index->vals =
        mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED,
             sorted_index->vals_fd, 0);

    sprintf(idx_data_path, "%s/sorted_pos", idx_path);
    sorted_index->pos_fd = open(idx_data_path, O_CREAT | O_RDWR, S_IRWXU);
    length = sizeof(size_t) * col->size;
    ftruncate(sorted_index->pos_fd, (off_t)length);
    sorted_index->pos =
        mmap(0, length, PROT_READ | PROT_WRITE, MAP_SHARED,
             sorted_index->pos_fd, 0);

    col->index.payload = sorted_index;
    init_sorted_index(col, NULL);
  }
}

/**
 * parse_create parses a create statement and then passes the necessary
 *arguments off to the next function
 **/
void parse_create(char* create_arguments) {
  char *tokenizer_copy, *to_free;
  // Since strsep destroys input, we create a copy of our input.
  tokenizer_copy = to_free = malloc((strlen(create_arguments) + 1));
  char* token;
  strcpy(tokenizer_copy, create_arguments);
  // check for leading parenthesis after create.
  if (strncmp(tokenizer_copy, "(", 1) == 0) {
    tokenizer_copy++;
    // token stores first argument. Tokenizer copy now points to just past first
    // ","
    token = strsep(&tokenizer_copy, ",");
    // pass off to next parse function.
    if (strcmp(token, "db") == 0) {
      parse_create_db(tokenizer_copy);
    } else if (strcmp(token, "tbl") == 0) {
      parse_create_tbl(tokenizer_copy);
    } else if (strcmp(token, "col") == 0) {
      parse_create_col(tokenizer_copy);
    } else if (strcmp(token, "idx") == 0) {
      parse_create_idx(tokenizer_copy);
    }
  }
  free(to_free);
}

DbOperator* parse_insert(char* query_command) {
  unsigned int columns_inserted = 0;
  char* token = NULL;
  // check for leading '('
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    char* db_name = strsep(command_index, ".");
    if (current_db == NULL || not_current_db(db_name))
      current_db = load_db(db_name);

    // parse table input
    char* table_name = strsep(command_index, ",");

    // lookup the table and make sure it exists.
    Table* insert_table = lookup_table(table_name);
    if (insert_table == NULL) return NULL;

    // make insert operator.
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = INSERT;
    dbo->operator_fields.insert_operator.table = insert_table;
    dbo->operator_fields.insert_operator.values =
        malloc(sizeof(int) * insert_table->col_count);
    // parse inputs until we reach the end. Turn each given string into an
    // integer.
    while ((token = strsep(command_index, ",")) != NULL) {
      int insert_val = atoi(token);
      dbo->operator_fields.insert_operator.values[columns_inserted] =
          insert_val;
      columns_inserted++;
    }
    // check that we received the correct number of input values
    if (columns_inserted != insert_table->col_count) {
      free(dbo);
      return NULL;
    }
    return dbo;
  } else {
    return NULL;
  }
}

DbOperator* parse_update(char* query_command, ClientContext* context) {
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    char* db_name = strsep(command_index, ".");
    if (current_db == NULL || not_current_db(db_name)) {
      log_err("Bad db name");
      return NULL;
    }

    char* tbl_name = strsep(command_index, ".");
    char* col_name = strsep(command_index, ",");
    char* pos_handle = strsep(command_index, ",");
    char* val_token = strsep(command_index, ")");

    Table* tbl = lookup_table(tbl_name);
    size_t col_idx = lookup_column_idx(tbl, col_name);
    Result* pos = lookup_handle_result(context, pos_handle);
    int update_val = atoi(val_token);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = UPDATE;
    dbo->operator_fields.update_operator.tbl = tbl;
    dbo->operator_fields.update_operator.col_idx = col_idx;
    dbo->operator_fields.update_operator.pos = pos;
    dbo->operator_fields.update_operator.update_val = update_val;
    return dbo;
  }
  return NULL;
}

DbOperator* parse_delete(char* query_command, ClientContext* context) {
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    char* db_name = strsep(command_index, ".");
    if (current_db == NULL || not_current_db(db_name)) {
      log_err("Bad db name");
      return NULL;
    }

    char* tbl_name = strsep(command_index, ",");
    char* pos_handle = strsep(command_index, ")");

    Table* table = lookup_table(tbl_name);
    Result* pos = lookup_handle_result(context, pos_handle);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = DELETE;
    dbo->operator_fields.delete_operator.table = table;
    dbo->operator_fields.delete_operator.pos = pos;
    return dbo;
  }
  return NULL;
}

void parse_batch(char* parse_arguments, ClientContext* context) {
  if (strncmp(parse_arguments, "_", 1) == 0) {
    parse_arguments++;
    char** command_index = &parse_arguments;
    char* batch_option = strsep(command_index, "(");

    BatchSelect* batch = context->batched_queries;

    if (strcmp(batch_option, "queries") == 0) {
      batch->is_collecting = true;
    } else if (strcmp(batch_option, "execute") == 0) {
      batch->is_collecting = false;
      shared_select(batch);

      for (size_t i = 0; i < batch->size; i++)
        update_context(context, batch->handles[i], batch->results[i]);

      free_batch(batch);
      context->batched_queries = init_batch();
    }
  }
}

DbOperator* parse_select(char* query_command, ClientContext* context) {
  char* token = NULL;
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    token = strsep(command_index, ".");
    if (token == NULL) return NULL;

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SELECT;

    Comparator* cmp = malloc(sizeof(Comparator));
    cmp->gen_col = malloc(sizeof(GeneralizedColumn));
    cmp->gen_col_id = NULL;

    // select from result -- <vec_pos>=select(<posn_vec>,<val_vec>,<low>,<high>)
    if (token[strlen(token) - 1] == ')') {
      command_index = &token;
      char* id_handle = strsep(command_index, ",");
      char* val_handle = strsep(command_index, ",");

      Result* ids_res = lookup_handle_result(context, id_handle);
      Result* vals_res = lookup_handle_result(context, val_handle);

      cmp->gen_col_id = malloc(sizeof(GeneralizedColumn));
      cmp->gen_col_id->column_type = RESULT;
      cmp->gen_col_id->column_pointer.result = ids_res;

      cmp->gen_col->column_type = RESULT;
      cmp->gen_col->column_pointer.result = vals_res;
    }
    // select from column -- <vec_pos>=select(<col_name>,<low>,<high>)
    else {
      char* db_name = token;
      if (current_db == NULL || not_current_db(db_name))
        current_db = load_db(db_name);
      char* tbl_name = strsep(command_index, ".");
      char* col_name = strsep(command_index, ",");

      cmp->gen_col->column_type = COLUMN;
      cmp->gen_col->column_pointer.column = lookup_column(tbl_name, col_name);
    }

    // parse low and/or high
    char* val_token = strsep(command_index, ",");
    if (strcmp(val_token, "null") == 0) {
      cmp->type1 = NO_COMPARISON;
      cmp->p_low = INT_MIN;
    } else {
      cmp->type1 = GREATER_THAN_OR_EQUAL;
      cmp->p_low = atoi(val_token);
    }

    val_token = strsep(command_index, ")");
    if (strcmp(val_token, "null") == 0) {
      cmp->type2 = NO_COMPARISON;
      cmp->p_high = INT_MAX;
    } else {
      cmp->type2 = LESS_THAN;
      cmp->p_high = atoi(val_token);
    }

    dbo->operator_fields.select_operator.comparator = cmp;
    return dbo;
  }
  return NULL;
}

DbOperator* parse_fetch(char* query_command, ClientContext* context) {
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    char* db_name = strsep(command_index, ".");
    if (current_db == NULL || not_current_db(db_name)) {
      log_err("Bad db name");
      return NULL;
    }

    char* tbl_name = strsep(command_index, ".");
    char* col_name = strsep(command_index, ",");
    char* pos_handle = strsep(command_index, ")");

    Column* vals_col = lookup_column(tbl_name, col_name);
    Result* ids_res = lookup_handle_result(context, pos_handle);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = FETCH;
    dbo->operator_fields.fetch_operator.vals_col = vals_col;
    dbo->operator_fields.fetch_operator.ids_res = ids_res;
    return dbo;
  }
  return NULL;
}

DbOperator* parse_join(char* query_command, ClientContext* context) {
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;

    char* val_handle_1 = strsep(command_index, ",");
    char* pos_handle_1 = strsep(command_index, ",");
    char* val_handle_2 = strsep(command_index, ",");
    char* pos_handle_2 = strsep(command_index, ",");
    char* join_type = strsep(command_index, ")");

    Result* val_1 = lookup_handle_result(context, val_handle_1);
    Result* pos_1 = lookup_handle_result(context, pos_handle_1);
    Result* val_2 = lookup_handle_result(context, val_handle_2);
    Result* pos_2 = lookup_handle_result(context, pos_handle_2);

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = JOIN;
    dbo->operator_fields.join_operator.val_l = val_1;
    dbo->operator_fields.join_operator.pos_l = pos_1;
    dbo->operator_fields.join_operator.val_r = val_2;
    dbo->operator_fields.join_operator.pos_r = pos_2;
    dbo->operator_fields.join_operator.join_type =
        (strcmp(join_type, "nested-loop") == 0) ? NESTED : HASH;
    return dbo;
  }
  return NULL;
}

DbOperator* parse_avg_sum(char* query_command, ClientContext* context,
                          bool is_avg) {
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = is_avg ? AVG : SUM;

  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;
    char* token = strsep(command_index, ".");
    int last_char = strlen(token) - 1;

    AvgSumOperator* op = malloc(sizeof(AvgSumOperator));
    op->gen_col = malloc(sizeof(GeneralizedColumn));

    if (token[last_char] == ')') {
      token[last_char] = '\0';
      Result* vals_res = lookup_handle_result(context, token);

      op->gen_col->column_type = RESULT;
      op->gen_col->column_pointer.result = vals_res;
    } else {
      char* db_name = token;
      char* table_name = strsep(command_index, ".");
      char* col_name = strsep(command_index, ")");
      if (current_db == NULL) load_db(db_name);
      Column* vals_col = lookup_column(table_name, col_name);

      op->gen_col->column_type = COLUMN;
      op->gen_col->column_pointer.column = vals_col;
    }

    dbo->operator_fields.avg_sum_operators = *op;
    free(op);
    return dbo;
  }
  return NULL;
}

DbOperator* parse_add_sub(char* query_command, ClientContext* context,
                          bool is_add) {
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = is_add ? ADD : SUB;

  AddSubOperator* op = malloc(sizeof(AddSubOperator));
  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;
    char* handle_a = strsep(command_index, ",");
    char* handle_b = strsep(command_index, ")");
    op->val_l = lookup_handle_result(context, handle_a);
    op->val_r = lookup_handle_result(context, handle_b);
    dbo->operator_fields.add_sub_operators = *op;
    free(op);
    return dbo;
  }
  return NULL;
}

DbOperator* parse_max_min(char* query_command, ClientContext* context,
                          bool is_max) {
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = is_max ? MAX : MIN;

  if (strncmp(query_command, "(", 1) == 0) {
    query_command++;
    char** command_index = &query_command;
    char* handle = strsep(command_index, ")");
    Result* vals_res = lookup_handle_result(context, handle);

    dbo->operator_fields.max_min_operators.vals_res = vals_res;
    return dbo;
  }
  return NULL;
}

DbOperator* parse_print(char* query_command) {
  DbOperator* dbo = malloc(sizeof(DbOperator));
  dbo->type = PRINT;
  PrintOperator* op = &(dbo->operator_fields.print_operator);
  op->handles = malloc(sizeof(char*));
  op->handle_num = 0;
  char* token;
  trim_parenthesis(query_command);
  while ((token = strsep(&query_command, ","))) {
    op->handles = realloc(op->handles, sizeof(char*) * (op->handle_num + 1));
    op->handles[op->handle_num] = malloc(strlen(token) + 1);
    strcpy(op->handles[op->handle_num], token);
    op->handle_num++;
  }
  return dbo;
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 **/
DbOperator* parse_command(char* query_command, int client_socket,
                          ClientContext* context) {
  DbOperator* dbo = NULL;

  char* equals_pointer = strchr(query_command, '=');
  char* handle = query_command;
  if (equals_pointer != NULL) {
    // handle exists, store here.
    *equals_pointer = '\0';
    cs165_log(stdout, "FILE HANDLE: %s\n", handle);
    query_command = ++equals_pointer;
  } else {
    handle = NULL;
  }

  cs165_log(stdout, "QUERY: %s\n", query_command);

  query_command = trim_whitespace(query_command);
  // check what command is given.
  if (strncmp(query_command, "create", 6) == 0) {
    query_command += 6;
    parse_create(query_command);
    dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
  } else if (strncmp(query_command, "relational_insert", 17) == 0) {
    query_command += 17;
    dbo = parse_insert(query_command);
  } else if (strncmp(query_command, "relational_update", 17) == 0) {
    query_command += 17;
    dbo = parse_update(query_command, context);
  } else if (strncmp(query_command, "relational_delete", 17) == 0) {
    query_command += 17;
    dbo = parse_delete(query_command, context);
  } else if (strncmp(query_command, "load", 4) == 0) {
    dbo = malloc(sizeof(DbOperator));
    dbo->type = LOAD;
  } else if (strncmp(query_command, "batch", 5) == 0) {
    query_command += 5;
    parse_batch(query_command, context);
  } else if (strncmp(query_command, "select", 6) == 0) {
    query_command += 6;
    dbo = parse_select(query_command, context);
    strcpy(dbo->operator_fields.select_operator.handle, handle);
  } else if (strncmp(query_command, "fetch", 5) == 0) {
    query_command += 5;
    dbo = parse_fetch(query_command, context);
    strcpy(dbo->operator_fields.fetch_operator.handle, handle);
  } else if (strncmp(query_command, "join", 4) == 0) {
    query_command += 4;
    dbo = parse_join(query_command, context);
    char** handle_index = &handle;
    char* handle_l = strsep(handle_index, ",");
    char* handle_r = *handle_index;
    strcpy(dbo->operator_fields.join_operator.handle_l, handle_l);
    strcpy(dbo->operator_fields.join_operator.handle_r, handle_r);
  } else if (strncmp(query_command, "avg", 3) == 0) {
    query_command += 3;
    dbo = parse_avg_sum(query_command, context, true);
    strcpy(dbo->operator_fields.avg_sum_operators.handle, handle);
  } else if (strncmp(query_command, "sum", 3) == 0) {
    query_command += 3;
    dbo = parse_avg_sum(query_command, context, false);
    strcpy(dbo->operator_fields.avg_sum_operators.handle, handle);
  } else if (strncmp(query_command, "add", 3) == 0) {
    query_command += 3;
    dbo = parse_add_sub(query_command, context, true);
    strcpy(dbo->operator_fields.add_sub_operators.handle, handle);
  } else if (strncmp(query_command, "sub", 3) == 0) {
    query_command += 3;
    dbo = parse_add_sub(query_command, context, false);
    strcpy(dbo->operator_fields.add_sub_operators.handle, handle);
  } else if (strncmp(query_command, "max", 3) == 0) {
    query_command += 3;
    dbo = parse_max_min(query_command, context, true);
    strcpy(dbo->operator_fields.max_min_operators.handle, handle);
  } else if (strncmp(query_command, "min", 3) == 0) {
    query_command += 3;
    dbo = parse_max_min(query_command, context, false);
    strcpy(dbo->operator_fields.max_min_operators.handle, handle);
  } else if (strncmp(query_command, "print", 5) == 0) {
    query_command += 5;
    dbo = parse_print(query_command);
  } else if (strncmp(query_command, "shutdown", 8) == 0) {
    dbo = malloc(sizeof(DbOperator));
    dbo->type = CLOSE;
  }
  if (dbo) {
    dbo->client_fd = client_socket;
    dbo->context = context;
  }
  return dbo;
}
