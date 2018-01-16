# Running

## Unit Testing
The unit tests do not require any additional setup or configuration. Simply run
the `cassandra_unit_tests` executable. 

This executable can be found in the
`test/unit_tests` directory under the `build` directory on Unix platforms. The `build`
directory is typically the current working directory when you run unit tests since that
is the directory where the build was launched from.

The `cassandra_unit_tests` executable is located in the `driver\test\unit_tests\<build flavor>` directory on
Windows. `<build flavor>` is either `Release` or `Debug`.

Unit tests are used to ensure the functionality of internal components and don't require an instance of Cassandra
to be running.

## Integration Testing
The integration tests require a machine (local or remote) with Secure Shell
(SSH) capabilities; either physical machine or Virtual Machine (VM). These
tests rely on [Cassandra Cluster Manager (CCM)] and
cannot run successfully if this dependency is not met. Once the [configuration]
has been completed the integration tests can be run by executing the
`cassandra_integration_tests` executable. 

This executable can be found in the `test/integration_tests` directory under the `build`
directory on Unix platforms. The `build` directory is typically the current working directory
when you run unit tests since that is the directory where the build was launched from.

The `cassandra_integration_tests` executable is located in the
`driver\test\integration_tests\<build flavor>` directory on Windows. `<build flavor>` is
either `Release` or `Debug`.

Integration tests are used to ensure the functionality of the driver's API and/or the interaction of multiple
internal components.

## Running Tests Individually
Running the unit or integration test executable without any arguments will start all
of the tests in sequence (one after another). The Boost Test
framework with which the integration and unit tests are compiled come with a
filter argument that allows for individual test execution. Using the
`--run_test` (or `-t` short option) argument, supplying the test suite and case will execute a single
test.

### Example
The example below shows how to execute the `simple` test case in the `async`
test suite of the integration tests.

```bash
./test/integration_tests/cassandra_integration_tests --run_test=async/simple
````

To run all of the test cases in the `async` suite, wildcards can be used in
conjunction with the `run_test` argument

```bash
./test/integration_tests/cassandra_integration_tests -t async/*
```

Equivalently, without wildcards all test cases within the suite run:

```bash
./test/integration_tests/cassandra_integration_tests -t async
```


[Cassandra Cluster Manager (CCM)]: http://datastax.github.io/cpp-driver/topics/testing/ccm/
[configuration]: http://datastax.github.io/cpp-driver/topics/testing/configuration/
