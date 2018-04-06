# Getting Started

## Installation

Directions for installing the driver can be found [here](/installation).

## Upgrading From The Core C/C++ Driver

Connecting the driver is is the same as the core [C/C++ driver](http://datastax.github.io/cpp-driver/)
except that `dse.h` should be included instead of `cassandra.h` which is
automatically included by `dse.h`.

```c
/* Include the DSE driver */
#include <dse.h>

int main() {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, "127.0.0.1");

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {

    /* Run queries here */

    /* Close the session */
    CassFuture* close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length,
                                                        message);
  }

  /* Cleanup driver objects */
  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
```

### Building against the DSE driver

Your application will need to link against the `dse` library. On Linux or OS X
your application will link against `dse.so` (or `dse_static.a` for static
linking). Here's how you would link on those platforms:

```
# cc example.c -I<path to dse.h> -L<path to dse.so> -ldse
```

On Windows your application will need to link against `dse.lib` (or
`dse_static.lib` for static linking). When linking against `dse.lib` your
application will also require `dse.dll` at runtime.

These libraries (`dse.lib` or `dse_static.lib`) can be added to your MS Visual
Studio project by adding them to project's properties under `Configuration
Properties/Linker/Input/Additional Dependencies`.
