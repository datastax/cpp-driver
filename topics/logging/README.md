# Logging

## Default

## Custom logging

The driver's logging system uses `stderr` by default and the log level `CASS_LOG_WARN`. Both of these settings can be changed using the following log configuration functions. A full example of using the logging API with a custom callback can be found [here](/examples/logging/logging.c). 

**Logging configuration must be done before calling any other driver function.**

To update the log level use `cass_log_set_level()`.

```c
cass_log_set_level(CASS_LOG_INFO);

/* Create cluster and connect session */

```

The logging callback allows an application to log messages to a file, syslog, or any other logging mechanism. This callback must be thread-safe because it is possible for it to be called from multiple threads concurrently. The `data` parameter allows custom resources to be passed to the logging callback.

```c
void on_log(const CassLogMessage* message, void* data) {
  /* Handle logging */
}

...

void* log_data = /* Custom log resource */;
cass_log_set_callback(on_log, log_data);
cass_log_set_level(CASS_LOG_INFO);

/* Create cluster and connect session */

```

Resources passed to a custom logging callback should be cleaned up after a call to `cass_log_cleanup()`. This shuts down the logging system and ensures that the custom callback will no longer be called.

```c
/* Close any sessions */

cass_log_cleanup();

/* Free custom logging resources */
```