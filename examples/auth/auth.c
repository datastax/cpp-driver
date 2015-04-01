/*
  This is free and unencumbered software released into the public domain.

  Anyone is free to copy, modify, publish, use, compile, sell, or
  distribute this software, either in source code form or as a compiled
  binary, for any purpose, commercial or non-commercial, and by any
  means.

  In jurisdictions that recognize copyright laws, the author or authors
  of this software dedicate any and all copyright interest in the
  software to the public domain. We make this dedication for the benefit
  of the public at large and to the detriment of our heirs and
  successors. We intend this dedication to be an overt act of
  relinquishment in perpetuity of all present and future rights to this
  software under copyright law.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
  OTHER DEALINGS IN THE SOFTWARE.

  For more information, please refer to <http://unlicense.org/>
*/

#include <cassandra.h>

#include <stdio.h>
#include <string.h>

typedef struct Credentials_ {
  const char* password;
  const char* username;
} Credentials;

size_t on_auth_initial(CassAuth* auth,
                       void* data,
                       char* response,
                       size_t response_size) {
  /*
   * This callback is used to initiate a request to begin an authentication
   * exchange. Required resources can be acquired and initialized here.
   *
   * Resources required for this specific exchange can be stored in the
   * auth->data field and will be available in the subsequent challenge
   * and success phases of the exchange. The cleanup callback should be used to
   * free these resources.
   */

  /*
   * The data parameter contains the credentials passed in when the
   * authentication callbacks were set and is available to all
   * authentication exchanges.
   */
  const Credentials* credentials = (const Credentials *)data;

  size_t username_size = strlen(credentials->username);
  size_t password_size = strlen(credentials->password);
  size_t size = username_size + password_size + 2;

  if (response_size < size) {
    return size;
  }

  /* Credentials are prefixed with '\0' */
  response[0] = '\0';
  memcpy(response + 1, credentials->username, username_size);

  response[username_size + 1] = '\0';
  memcpy(response + username_size + 2, credentials->password, password_size);

  return size;
}

size_t on_auth_challenge(CassAuth* auth,
                         void* data,
                         const char* challenge,
                         size_t challenge_size,
                         char* response,
                         size_t response_size) {
  /*
   * Not used for plain text authentication, but this is to be used
   * for handling an authentication challenge initiated by the server.
   */
  return 0;
}

void on_auth_success(CassAuth* auth,
                     void* data,
                     const char* token,
                     size_t token_size ) {
  /*
   * Not used for plain text authentication, but this is to be used
   * for handling the success phase of an exchange.
   */
}

void on_auth_cleanup(CassAuth* auth, void* data) {
  /*
   * No resources cleanup is necessary for plain text authentication, but
   * this is used to cleanup resources acquired during the authentication
   * exchange.
   */
}

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Setup authentication callbacks and credentials */
  CassAuthCallbacks auth_callbacks = {
    on_auth_initial,
    on_auth_challenge,
    on_auth_success,
    on_auth_cleanup
  };

  Credentials credentials = {
    "cassandra",
    "cassandra"
  };

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "127.0.0.1,127.0.0.2,127.0.0.3");

  /* Set custom authentication callbacks and credentials */
  cass_cluster_set_auth_callbacks(cluster, &auth_callbacks, &credentials);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    printf("Successfully connected!\n");

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    CassString message = cass_future_error_message(connect_future);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message.length,
                                                        message.data);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
