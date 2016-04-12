# Authentication

The driver can be setup to use either the built-in plain text authentication
or to use a custom authentication implementation.

## Plain text

Credentials are provided using the [`cass_cluster_set_credentials()`] function.

```c
cass_cluster_set_credentials(cluster, "cassandra", "cassandra");
```

**Important**: The credentials are sent in plain text to the server. For this
reason, it is highly recommended that this be used in conjunction with
client-to-node encryption (SSL), or in a trusted network environment.

## Custom

A custom authentication implementation can be set using
[`cass_cluster_set_auth_callbacks()`]. This is useful for integrating with more
complex authentication systems such as Kerberos.

```c
size_t on_auth_initial(CassAuth* auth,
                       void* data,
                       char* response,
                       size_t response_size) {
  /*
   * This callback is used to initiate a request to begin an authentication
   * exchange. Required resources can be acquired and initialized here.
   *
   * Resources required for this specific exchange can be stored in the
   * `auth->data` field and will be available in the subsequent challenge
   * and success phases of the exchange. The cleanup callback should be used to
   * free these resources. This data will only ever be accessed by a single
   * thread.
   */

   /*
    * Return the number of bytes copied into `response` or the required size
    * of `response` if it's too small.
    */
   return num_bytes;
}

size_t on_auth_challenge(CassAuth* auth,
                         void* data,
                         const char* token,
                         size_t token_size,
                         char* response,
                         size_t response_size) {
  /*
   * This is used for handling an authentication challenge initiated
   * by the server. The information contained in the token parameter is
   * authentication protocol specific. It may be NULL or empty.
   */

   /*
    * Return the number of bytes copied into `response` or the required size
    * of `response` if it's too small.
    */
  return num_bytes;
}

void on_auth_success(CassAuth* auth,
                     void* data,
                     const char* token,
                     size_t token_size ) {
  /*
   * This is to be used for handling the success phase of an exchange. The
   * token parameters contains information that may be used to finialize
   * the request. The information contained in the token parameter is
   * authentication protocol specific. It may be NULL or empty.
   */
}

void on_auth_cleanup(CassAuth* auth, void* data) {
  /*
   * This is used to cleanup resources acquired during the authentication
   * exchange.
   */
}

/* ... */

int main() {
  /* ... */

  /* Initialize the callback struct */
  CassAuthCallbacks auth_callbacks = {
    on_auth_initial,
    on_auth_challenge,
    on_auth_success,
    on_auth_cleanup
  };


  /*
   * The `data` argument passed into `cass_cluster_set_auth_callbacks()`
   * is passed as the `data` parameter into the authentication callbacks.
   * Callbacks will be called by multiple threads concurrently so it is important
   * makes sure this data is either immutable or its access is serialized.
   */
  void* data = /* Some data */;

  /* Set custom authentication callbacks and callback data */
  cass_cluster_set_auth_callbacks(cluster, &auth_callbacks, data);

  /* ... */

  return 0;
}
```

[`cass_cluster_set_credentials()`]: http://datastax.github.io/cpp-driver/api/struct_cass_cluster/#1a44848bd5231e0e525fdfa5c5f4c37ff8
[`cass_cluster_set_auth_callbacks()`]: http://datastax.github.io/cpp-driver/api/struct_cass_cluster/#1aba62ec73570702e5044c547ceca23da5
