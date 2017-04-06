/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "dse.h"

void print_error(CassFuture* future) {
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}

void precision_to_string(DseDateRangePrecision precision, char* precision_string) {
  switch (precision) {
    case DSE_DATE_RANGE_PRECISION_YEAR:
      strcpy(precision_string, "YEAR");
      break;
    case DSE_DATE_RANGE_PRECISION_MONTH:
      strcpy(precision_string, "MONTH");
      break;
    case DSE_DATE_RANGE_PRECISION_DAY:
      strcpy(precision_string, "DAY");
      break;
    case DSE_DATE_RANGE_PRECISION_HOUR:
      strcpy(precision_string, "HOUR");
      break;
    case DSE_DATE_RANGE_PRECISION_MINUTE:
      strcpy(precision_string, "MINUTE");
      break;
    case DSE_DATE_RANGE_PRECISION_SECOND:
      strcpy(precision_string, "SECOND");
      break;
    case DSE_DATE_RANGE_PRECISION_MILLISECOND:
      strcpy(precision_string, "MILLISECOND");
      break;
    default:
      strcpy(precision_string, "UNKNOWN");
  }
}

void time_to_string(cass_int64_t time_int, char* time_string) {
  /* time_int is ms-precision. */
  time_t time_secs = (time_t) time_int / 1000;
  strftime(time_string, 20, "%Y-%m-%d %H:%M:%S", gmtime(&time_secs));
  sprintf(time_string + 19, ".%03d", (int) (time_int % 1000));
}

void print_range(const DseDateRange* range) {
  char from_precision_string[20], to_precision_string[20], from_time_string[30], to_time_string[30];

  if (range->is_single_date) {
    if (dse_date_range_bound_is_unbounded(range->lower_bound)) {
      printf("*\n");
    } else {
      precision_to_string(range->lower_bound.precision, from_precision_string);
      time_to_string(range->lower_bound.time_ms, from_time_string);
      printf("%s(%s)\n", from_time_string, from_precision_string);
    }
  } else if (dse_date_range_bound_is_unbounded(range->lower_bound) &&
             dse_date_range_bound_is_unbounded(range->upper_bound)) {
    printf("* TO *\n");
  } else if (dse_date_range_bound_is_unbounded(range->upper_bound)) {
    precision_to_string(range->lower_bound.precision, from_precision_string);
    time_to_string(range->lower_bound.time_ms, from_time_string);
    printf("%s(%s) TO *\n", from_time_string, from_precision_string);
  } else if (dse_date_range_bound_is_unbounded(range->lower_bound)) {
    precision_to_string(range->upper_bound.precision, to_precision_string);
    time_to_string(range->upper_bound.time_ms, to_time_string);
    printf("* TO %s(%s)\n", to_time_string, to_precision_string);
  } else {
    precision_to_string(range->lower_bound.precision, from_precision_string);
    time_to_string(range->lower_bound.time_ms, from_time_string);
    precision_to_string(range->upper_bound.precision, to_precision_string);
    time_to_string(range->upper_bound.time_ms, to_time_string);
    printf("%s(%s) TO %s(%s)\n", from_time_string, from_precision_string, to_time_string, to_precision_string);
  }
}

CassCluster* create_cluster(const char* hosts) {
  CassCluster* cluster = cass_cluster_new();
  cass_cluster_set_contact_points(cluster, hosts);
  cass_cluster_set_dse_plaintext_authenticator(cluster, "cassandra", "cassandra");
  return cluster;
}

CassError connect_session(CassSession* session, const CassCluster* cluster) {
  CassError rc = CASS_OK;
  CassFuture* future = cass_session_connect(session, cluster);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }
  cass_future_free(future);

  return rc;
}

CassError execute_query(CassSession* session, const char* query) {
  CassError rc = CASS_OK;
  CassFuture* future = NULL;
  CassStatement* statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError insert_into_table(CassSession* session,
                            const char* key,
                            const DseDateRange* range) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "INSERT INTO examples.dr (key, value) VALUES (?, ?);";

  statement = cass_statement_new(query, 2);

  cass_statement_bind_string(statement, 0, key);
  cass_statement_bind_dse_date_range(statement, 1, range);

  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError select_from_table(CassSession* session) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT * FROM examples.dr";

  statement = cass_statement_new(query, 0);

  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    while (cass_iterator_next(iterator)) {
      const char* row_key;
      size_t row_key_length;
      const CassRow* row = cass_iterator_get_row(iterator);
      int rc = 0;
      DseDateRange range;

      if ((rc = cass_value_get_string(cass_row_get_column(row, 0), &row_key, &row_key_length)) == CASS_OK) {
        printf("%.*s\t", (int) row_key_length, row_key);
      } else {
        printf("got error: %d\n", rc);
      }

      if ((rc = cass_value_get_dse_date_range(cass_row_get_column(row, 1), &range)) == CASS_OK) {
        print_range(&range);
      } else {
        printf("got error: %d\n", rc);
      }
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError insert_into_collections(CassSession* session, const char* key, const DseDateRange* range1, const DseDateRange* range2) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  CassCollection* collection = NULL;
  CassTuple* tuple = NULL;
  const CassSchemaMeta* schema_meta = NULL;
  const CassKeyspaceMeta* keyspace_meta = NULL;
  const CassDataType* udt_type = NULL;
  CassUserType* udt = NULL;

  const char** item = NULL;
  const char* query = "INSERT INTO examples.drcoll (key, coll_value, tuple_value, udt_value) VALUES (?, ?, ?, ?);";
  int ind = 0;

  statement = cass_statement_new(query, 4);

  cass_statement_bind_string(statement, 0, key);

  /* Set up a collection. */
  collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, 2);
  cass_collection_append_dse_date_range(collection, range1);
  cass_collection_append_dse_date_range(collection, range2);

  cass_statement_bind_collection(statement, 1, collection);
  cass_collection_free(collection);

  /* Set up a tuple. */
  tuple = cass_tuple_new(2);
  cass_tuple_set_dse_date_range(tuple, 0, range2);
  cass_tuple_set_dse_date_range(tuple, 1, range1);

  cass_statement_bind_tuple(statement, 2, tuple);
  cass_tuple_free(tuple);

  /* Set up the udt. */
  schema_meta = cass_session_get_schema_meta(session);
  keyspace_meta = cass_schema_meta_keyspace_by_name(schema_meta, "examples");
  udt_type = cass_keyspace_meta_user_type_by_name(keyspace_meta, "dr_user_type");
  udt = cass_user_type_new_from_data_type(udt_type);
  cass_user_type_set_dse_date_range_by_name(udt, "sub", range1);

  cass_statement_bind_user_type(statement, 3, udt);
  cass_user_type_free(udt);
  cass_schema_meta_free(schema_meta);

  /* Insert the row. */
  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

CassError select_from_collections(CassSession* session, const char* key) {
  CassError rc = CASS_OK;
  CassStatement* statement = NULL;
  CassFuture* future = NULL;
  const char* query = "SELECT coll_value, tuple_value, udt_value FROM examples.drcoll WHERE key = ?";

  statement = cass_statement_new(query, 1);
  cass_statement_bind_string(statement, 0, key);

  future = cass_session_execute(session, statement);

  rc = cass_future_error_code(future);
  if (rc != CASS_OK) {
    print_error(future);
  } else {
    const CassResult* result = cass_future_get_result(future);
    CassIterator* iterator = cass_iterator_from_result(result);

    if (cass_iterator_next(iterator)) {
      const CassValue* value = NULL;
      const CassRow* row = cass_iterator_get_row(iterator);
      CassIterator* items_iterator = NULL;

      /* Get coll_value and print it out. */
      value = cass_row_get_column(row, 0);
      items_iterator = cass_iterator_from_collection(value);
      printf("coll_value:\n");
      while (cass_iterator_next(items_iterator)) {
        DseDateRange range;
        cass_value_get_dse_date_range(cass_iterator_get_value(items_iterator), &range);
        printf("  ");
        print_range(&range);
      }
      cass_iterator_free(items_iterator);

      /* Get tuple_value and print it out. */
      value = cass_row_get_column(row, 1);
      items_iterator = cass_iterator_from_tuple(value);
      printf("tuple_value:\n");
      while (cass_iterator_next(items_iterator)) {
        DseDateRange range;
        cass_value_get_dse_date_range(cass_iterator_get_value(items_iterator), &range);
        printf("  ");
        print_range(&range);
      }
      cass_iterator_free(items_iterator);

      /* Get udt_value */
      value = cass_row_get_column(row, 2);
      items_iterator = cass_iterator_fields_from_user_type(value);
      printf("udt_value:\n");
      while(items_iterator != NULL && cass_iterator_next(items_iterator)) {
        const char* field_name;
        size_t field_name_length;
        const CassValue* field_value = NULL;
        DseDateRange range;
        cass_iterator_get_user_type_field_name(items_iterator, &field_name, &field_name_length);
        field_value = cass_iterator_get_user_type_field_value(items_iterator);
        printf("  %.*s ", (int)field_name_length, field_name);
        cass_value_get_dse_date_range(field_value, &range);
        print_range(&range);
      }
      cass_iterator_free(items_iterator);

      printf("\n");
    }

    cass_result_free(result);
    cass_iterator_free(iterator);
  }

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

int main(int argc, char* argv[]) {
  CassCluster* cluster = NULL;
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  time_t now = time(NULL);
  DseDateRange range, range2;

  if (argc > 1) {
    hosts = argv[1];
  }
  cluster = create_cluster(hosts);

  if (connect_session(session, cluster) != CASS_OK) {
    cass_cluster_free(cluster);
    cass_session_free(session);
    return -1;
  }

  execute_query(session,
                "CREATE KEYSPACE IF NOT EXISTS examples WITH replication = { \
                           'class': 'SimpleStrategy', 'replication_factor': '1' };");


  execute_query(session,
                "CREATE TABLE IF NOT EXISTS examples.dr (key text PRIMARY KEY, \
                                           value 'DateRangeType');");

  execute_query(session,
                "CREATE TYPE IF NOT EXISTS examples.dr_user_type (sub 'DateRangeType')");

  execute_query(session,
                "CREATE TABLE IF NOT EXISTS examples.drcoll (key text PRIMARY KEY, \
                                               coll_value set<'DateRangeType'>, \
                                               tuple_value tuple<'DateRangeType', 'DateRangeType'>, \
                                               udt_value dr_user_type)");

  /* Insert a different flavors of date ranges into examples.dr. */
  insert_into_table(session, "open range", dse_date_range_init(&range,
                                                               dse_date_range_bound_unbounded(),
                                                               dse_date_range_bound_unbounded()));
  insert_into_table(session, "open value", dse_date_range_init_single_date(&range, dse_date_range_bound_unbounded()));
  insert_into_table(session, "single value",
                    dse_date_range_init_single_date(&range,
                                                    dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_MONTH, ((cass_int64_t) now) * 1000)));
  insert_into_table(session, "open high, day",
                    dse_date_range_init(&range,
                                        dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_DAY, ((cass_int64_t) now) * 1000),
                                        dse_date_range_bound_unbounded()));
  insert_into_table(session, "open low, ms",
                    dse_date_range_init(&range,
                                        dse_date_range_bound_unbounded(),
                                        dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_MILLISECOND, ((cass_int64_t) now) * 1000)));

  /* Closed range from 1/2/1970 to now (with some millis tacked on to show that millis are handled properly). */
  insert_into_table(session, "closed range",
                    dse_date_range_init(&range,
                                        dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_YEAR, 86400000),
                                        dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_MILLISECOND, ((cass_int64_t) now) * 1000 + 987)));

  /* Now query examples.dr and print out the results. */
  printf("examples.dr:\n");
  select_from_table(session);

  /* Insert a row in the collection table. */

  dse_date_range_init(&range,
                      dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_DAY, 86400000),
                      dse_date_range_bound_init(DSE_DATE_RANGE_PRECISION_MILLISECOND, ((cass_int64_t) now) * 1000 + 123));
  dse_date_range_init_single_date(&range2, dse_date_range_bound_unbounded());

  insert_into_collections(session, "key", &range, &range2);

  /* Query the collection table and print out the results. */
  printf("\n\nexamples.drcoll:\n");
  select_from_collections(session, "key");

  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}
