# The `date` and `time` Types

**Note**: Cassandra 2.2+ is required.

The driver currently uses raw types to handle `date` and `time` because date
and time handling is often very application specific in C/C++. It currently
provides methods to handle converting Epoch (January 1, 1970) time in seconds
to and from `date`/`time`.

The `date` type uses an unsigned 32-bit integer (`cass_uint32_t`) to
represent the number of days with Epoch centered at 2^31.
Because it's centered at Epoch it can be used to represent days before Epoch.
The `time` type uses a signed 64-bit integer (`cass_int64_t`) to
represent the number of nanoseconds since midnight and valid values are in the
range 0 to 86399999999999.


The following examples both use this schema:

```cql
CREATE TABLE date_time (key text PRIMARY KEY,
                        year_month_day date,
                        time_of_day time);
```

## `INSERT`ing the `date` and `time` Types

```c
#include <time.h>

void insert_date_time(CassSession* session) {

  CassStatement* statement = cass_statement_new("INSERT INTO date_time (key, year_month_day, time_of_day) "
                                                "VALUES (?, ?, ?)", 3);

  time_t now = time(NULL); /* Time in seconds from Epoch */

  /* Converts the time since the Epoch in seconds to the 'date' type */
  cass_uint32_t year_month_day = cass_date_from_epoch(now);

  /* Converts the time since the Epoch in seconds to the 'time' type */
  cass_int64_t time_of_day = cass_time_from_epoch(now);

  cass_statement_bind_string(statement, 0, "xyz");

  /* 'date' uses an unsigned 32-bit integer */
  cass_statement_bind_uint32(statement, 1, year_month_day);

  /* 'time' uses a signed 64-bit integer */
  cass_statement_bind_int64(statement, 2, time_of_day);

  CassFuture* future = cass_session_execute(session, statement);

  /* Handle future result */

  /* CassStatement and CassFuture both need to be freed */
  cass_statement_free(statement);
  cass_future_free(future);
}
```

## `SELECT`ing the `date` and `time` Types

```c
#include <time.h>

void select_date_time(CassSession* session) {
  CassStatement* statement = cass_statement_new("SELECT * FROM date_time WHERE key = ?", 1);

  CassFuture* future = cass_session_execute(session, statement);

  const CassResult* result = cass_future_get_result(future);
  /* Make sure there's a valid result */
  if (result != NULL && cass_result_row_count(result) > 0) {
    const CassRow* row = cass_result_first_row(result);

    /* Get the value of the "year_month_day" column */
    cass_uint32_t year_month_day;
    cass_value_get_uint32(cass_row_get_column(row, 1), &year_month_day);

    /* Get the value of the "time_of_day" column */
    cass_int64_t time_of_day;
    cass_value_get_int64(cass_row_get_column(row, 2), &time_of_day);

    /* Convert 'date' and 'time' to Epoch time */
    time_t time = (time_t)cass_date_time_to_epoch(year_month_day, time_of_day);
    printf("Date and time: %s", asctime(localtime(&time)));
  } else {
    /* Handle error */
  }

  /* CassStatement and CassFuture both need to be freed */
  cass_statement_free(statement);
  cass_future_free(future);
}
```
