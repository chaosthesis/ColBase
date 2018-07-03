/* This line at the top is necessary for compilation on the lab machine and many
other Unix machines. Please look up _XOPEN_SOURCE for more details. As well, if
your code does not compile on the lab machine please look into this as a a
source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2017
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
  int client_socket;
  size_t len;
  struct sockaddr_un remote;

  log_info("Attempting to connect...\n");

  if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  remote.sun_family = AF_UNIX;
  strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
  if (connect(client_socket, (struct sockaddr*)&remote, len) == -1) {
    log_err("client connect failed: ");
    return -1;
  }

  log_info("Client connected at socket: %d.\n", client_socket);
  return client_socket;
}

void send_file_per_request(char* load_arguments, int client_socket) {
  if (strncmp(load_arguments, "load", 4) == 0) {
    load_arguments += 4;
    char* file_path = trim_quotes(trim_parenthesis(load_arguments));
    file_path[strcspn(file_path, "\n")] = '\0';
    cs165_sendfile(file_path, client_socket);
  }
}

int main(void) {
  int client_socket = connect_client();
  if (client_socket < 0) exit(1);

  // Always output an interactive marker at the start of each command if the
  // input is from stdin. Do not output if piped in from file or from other fd
  char* prefix = "";
  if (isatty(fileno(stdin))) prefix = "db_client > ";

  char* output_str = NULL;

  // Continuously loop and wait for input. At each iteration:
  // 1. output interactive marker
  // 2. read from stdin until eof.
  char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
  message send_message;
  send_message.payload = read_buffer;
  send_message.status = 0;

  while (printf("%s", prefix),
         output_str = fgets(read_buffer, DEFAULT_STDIN_BUFFER_SIZE, stdin),
         !feof(stdin)) {
    if (output_str == NULL) {
      log_err("fgets failed.\n");
      break;
    }

    // Ignore comments
    if (strncmp(read_buffer, "--", 2) == 0) continue;

    // Send message to the server.
    send_message.length = strlen(read_buffer);
    if (send_message.length > 1) {
      cs165_send(client_socket, send_message);
      send_file_per_request(send_message.payload, client_socket);
      cs165_receive(client_socket);
    }
  }
  close(client_socket);
  return 0;
}
