# Logging

The driver's logging system uses `stderr` by default and the log level `CASS_LOG_WARN`. Both of these settings can be changed using the driver's `cass_log_*()` configuration functions.

**Important**: Logging configuration must be done before calling any other driver function.

## Log Level

To update the log level use `cass_log_set_level()`.

```c
cass_log_set_level(CASS_LOG_INFO);

/* Create cluster and connect session */
```

## Custom Logging Callback

The use of a logging callback allows an application to log messages to a file, syslog, or any other logging mechanism. This callback must be thread-safe because it is possible for it to be called from multiple threads concurrently. The `data` parameter allows custom resources to be passed to the logging callback.

```c
void on_log(const CassLogMessage* message, void* data) {
  /* Handle logging */
}

int main() {
  void* log_data = NULL /* Custom log resource */;
  cass_log_set_callback(on_log, log_data);
  cass_log_set_level(CASS_LOG_INFO);

  /* Create cluster and connect session */

}
```
