# Futures

Futures are returned from any driver call that would have required your application to block. This allows your application to continue processing and/or also submit several queries at once. Although the driver has an asynchronous design it can be used sychronously by immediately attempting to get result or explicting waiting for the future.

## Waiting for results

Futures results can be waited on indefinately by either calling a wait method or by attempting to get the result. They can also be waited on for an explict amount of time or they can polled without waiting.

### Synchronously waiting
```c
CassFuture* future = /* Some operation */

/* Block until a result or error is set */
cass_future_wait(future);

cass_future_free(future);
```

### Synchronously waiting for the result
```c
CassFuture* future = cass_session_execute(session, statement);

/* Blocks and gets a result */
const CassResult* result = cass_future_get_result(future);

/* If there was an error then the result won't be available */
if (result == NULL) {
  /* The error code and message will be set instead */
  CassError error_code = cass_future_error_code(future);
  CassString error_message = cass_future_error_message(future);

  /* Handle error */
}

cass_future_free(future);
```

### Timed wait
```c
CassFuture* future = /* Some operation */

cass_duration_t microseconds = 30 * 1000000; /* 30 seconds */

/* Block for a fixed amount of time */
if (cass_future_wait_timed(future, microseconds) {
  /* A result or error was set during the wait call */
} else {
  /* The operation hasn't completed yet */
}

cass_future_free(future);
```

### Polling

```c
CassFuture* future = /* Some operation */

/* Poll to see if the future is ready */
while (!cass_future_ready(future)) {
  /* Run other application logic or wait*/
}

/* A result or error was set */

cass_future_free(future);
```

## Callbacks

A callback can be set on a future to notify your application when a request has completed. Using a future callback is the lowest latency method of notification when waiting for several asynchronous operations.  

**Important**: The driver may run the callback on thread that’s different from the application’s calling thread. Any data accessed in the callback must be immutable or synchronized with a mutex, semaphore, etc. 

```c
void on_result(CassFuture* future, void* data) {
  /* This result will now return immediately */
  CassError rc = cass_future_error_code(future);
  printf("%s\n", cass_error_desc(rc));
}

...
 
CassFuture* future = /* Some operation */;
 
/* Set a callback instead of waiting for the result to be returned */
cass_future_set_callback(on_result, NULL);
 
/* The application's reference to the future can be freed immediately */
cass_future_free(future);
 
/* Run other application logic */
```

