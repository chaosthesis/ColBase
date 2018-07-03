#ifndef MESSAGE_H__
#define MESSAGE_H__

// mesage_status defines the status of the previous request.
typedef enum message_status {
  OK_DONE,
  OK_WAIT_FOR_RESPONSE,
  UNKNOWN_COMMAND,
  QUERY_UNSUPPORTED,
  OBJECT_ALREADY_EXISTS,
  OBJECT_NOT_FOUND,
  INCORRECT_FORMAT,
  EXECUTION_ERROR,
  INCORRECT_FILE_FORMAT,
  FILE_NOT_FOUND,
  INDEX_ALREADY_EXISTS
} message_status;

// message is a single packet of information sent between client/server.
// message_status: defines the status of the message.
// length: defines the length of the string message to be sent.
// payload: defines the payload of the message.
typedef struct message {
  message_status status;
  int length;
  char* payload;
} message;

void cs165_send(int client_socket, message send_message);
void cs165_receive(int client_socket);
void cs165_sendfile(char* file_path, int client_socket);

#endif
