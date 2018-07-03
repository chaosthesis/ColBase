#define _XOPEN_SOURCE
#define _BSD_SOURCE

#include <fcntl.h>
#include <stdio.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "cs165_api.h"
#include "message.h"
#include "utils.h"

void cs165_send(int client_socket, message send_message) {
  // Send the message_header, which tells the payload size
  if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
    log_err("Failed to send message header: %m");
    exit(1);
  }

  // Send the payload
  if (send_message.length > 0 &&
      send(client_socket, send_message.payload, send_message.length, 0) == -1) {
    log_err("Failed to send message payload: %m");
    exit(1);
  }
}

void cs165_receive(int client_socket) {
  message recv_message;
  int len = 0;

  // Always wait for server response (even if it is just an OK message)
  if ((len = recv(client_socket, &(recv_message), sizeof(message), 0)) > 0) {
    if ((recv_message.status == OK_WAIT_FOR_RESPONSE ||
         recv_message.status == OK_DONE) &&
        (int)recv_message.length > 0) {
      // Calculate number of bytes in response package
      int num_bytes = (int)recv_message.length;
      char payload[num_bytes + 1];

      // Receive the whole payload and print it out
      while ((len = recv(client_socket, payload, num_bytes, 0)) > 0) {
        payload[len] = '\0';
        printf("%s", payload);
        num_bytes -= len;
        if (num_bytes <= 0) break;
      }
      printf("\n");
    }
  } else {
    if (len < 0) {
      log_err("Failed to receive message.");
      exit(1);
    } else {
      log_info("Server closed connection\n");
      exit(0);
    }
  }
}

void cs165_sendfile(char* file_path, int client_socket) {
  struct stat stat_buf;
  int fd = open(file_path, O_RDONLY);

  // get file size
  fstat(fd, &stat_buf);
  off_t offset = 0;

  // send file
  if (sendfile(client_socket, fd, &offset, stat_buf.st_size) == -1) {
    log_err("Failed to send file: %m");
    exit(1);
  }
  close(fd);

  // send termination signal
  char* eof = "EOF\n";
  if (send(client_socket, eof, sizeof(eof), 0) == -1) {
    log_err("Failed to send termination signal: %m");
    exit(1);
  }
}