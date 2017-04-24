# C/C++ DataStax Enterprise Driver

[![Build Status: Linux](https://travis-ci.org/datastax/cpp-driver-dse.svg?branch=master)](https://travis-ci.org/datastax/cpp-driver-dse)
[![Build Status: Windows](https://ci.appveyor.com/api/projects/status/582057mqa3t6eimk/branch/master?svg=true)](https://ci.appveyor.com/project/DataStax/cpp-driver-dse)

This driver is built on top of the [C/C++ driver for Apache
Cassandra][cpp-driver], with specific extensions for DataStax Enterprise (DSE).

This software can be used solely with DataStax Enterprise. See the [License
section](#licence) below.

## Features

* [DSE plaintext and GSSAPI authentication](http://docs.datastax.com/en/developer/cpp-driver-dse/1.0/features/authentication/)
* [DSE geospatial types](http://docs.datastax.com/en/developer/cpp-driver-dse/1.0/features/geotypes/)
* [DSE graph integration](http://docs.datastax.com/en/developer/cpp-driver-dse/1.0/features/graph/)

## Documentation

Documentation for the DSE driver is found [here](http://docs.datastax.com/en/developer/cpp-driver-dse/).

## Examples

The driver includes several examples in the [examples](examples) directory for
using DSE specific features.

## A Simple Example

Connecting the driver is is the same as the core C/C++ driver except that
`dse.h` should be included instead of `cassandra.h` which is automatically
included by `dse.h`.

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
    close_future = cass_session_close(session);
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

## License

Copyright &copy; 2017 DataStax, Inc.

http://www.datastax.com/terms/datastax-dse-driver-license-terms

[cpp-driver]: http://datastax.github.io/cpp-driver/
