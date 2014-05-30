For advanced users only.

The suppression rules included in `suppress.txt' may be helpful when debugging the C++ driver for Cassandra.
You can use them with Valgrind, e.g. by:

valgrind --tool=memcheck --suppressions=suppress.txt  ./cql_integration_tests --run_test=<TEST_SUITE>/<TEST_CASE>

The rules were created under OS X with certain versions of all dependencies. Portability is not guaranteed.
