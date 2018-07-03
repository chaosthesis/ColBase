#ifndef PARSE_H__
#define PARSE_H__

#include "client_context.h"
#include "cs165_api.h"
#include "message.h"

DbOperator* parse_command(char* query_command, int client,
                          ClientContext* context);

void parse_load(char* load_arguments, int client_socket);

#endif
