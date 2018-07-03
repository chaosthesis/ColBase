/** server.c
 * CS165 Fall 2017
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include "client_context.h"
#include "common.h"
#include "cs165_api.h"
#include "db_manager.h"
#include "execute.h"
#include "message.h"
#include "parse.h"
#include "utils.h"

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
bool handle_client(int client_socket) {
  int done = 0;
  int length = 0;
  bool shutdown = false;

  log_info("Connected to socket: %d.\n", client_socket);

  // Create two messages, one from which to read and one from which to receive
  message send_message;
  message recv_message;

  // create the client context here
  ClientContext* client_context = init_context();

  // Continually receive messages from client and execute queries.
  // 1. Parse the command
  // 2. Handle request if appropriate
  // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
  // 4. Send response of request.
  do {
    length = recv(client_socket, &recv_message, sizeof(message), 0);
    if (length < 0) {
      log_err("Client connection closed!\n");
      exit(1);
    } else if (length == 0) {
      done = 1;
    }

    if (!done) {
      char recv_buffer[recv_message.length + 1];
      length = recv(client_socket, recv_buffer, recv_message.length, 0);
      if (strncmp(recv_buffer, "shutdown", 8) == 0) shutdown = true;

      recv_message.payload = recv_buffer;
      recv_message.payload[recv_message.length] = '\0';

      // Parse command
      DbOperator* query =
          parse_command(recv_message.payload, client_socket, client_context);

      // Handle request
      char* result = execute_DbOperator(query);

      // Send message header and message payload
      send_message.length = strlen(result);
      send_message.payload = result;
      send_message.status = OK_DONE;
      cs165_send(client_socket, send_message);

      if (send_message.length > 0) {
        free(send_message.payload);
      }
    }
  } while (!done);

  log_info("Connection closed at socket %d!\n", client_socket);

  free_context(client_context);
  free(client_context);

  close(client_socket);

  return shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
  int server_socket;
  size_t len;
  struct sockaddr_un local;
  log_info("Attempting to setup server...\n");

  if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
    log_err("L%d: Failed to create socket.\n", __LINE__);
    return -1;
  }

  local.sun_family = AF_UNIX;
  strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
  unlink(local.sun_path);

  len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
  if (bind(server_socket, (struct sockaddr*)&local, len) == -1) {
    log_err("L%d: Socket failed to bind.\n", __LINE__);
    return -1;
  }

  if (listen(server_socket, 5) == -1) {
    log_err("L%d: Failed to listen on socket.\n", __LINE__);
    return -1;
  }

  return server_socket;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You will need to extend this to handle multiple concurrent clients
// and remain running until it receives a shut-down command.
int main(void) {
  int server_socket = setup_server();
  if (server_socket < 0) {
    exit(1);
  }

  bool done = false;
  while (!done) {
    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);
    int client_socket = 0;

    if ((client_socket =
             accept(server_socket, (struct sockaddr*)&remote, &t)) == -1) {
      log_err("L%d: Failed to accept a new connection.\n", __LINE__);
      exit(1);
    }

    done = handle_client(client_socket);
  }

  return 0;
}
