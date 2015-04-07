# Running

## Unit Testing
The unit tests do not require any additional setup or configuration. Simply run
the `cassandra_unit_tests` executable. Unit tests are used to ensure the
functionality of internal components and don't require an instance of Cassandra
to be running.

## Integration Testing
The integration tests require a machine (local or remote) with Secure Shell
(SSH) capabilities; either physical machine or Virtual Machine (VM). These
tests rely on [Cassandra Cluster Manager (CCM)] and
cannot run successfully if this dependency is not met. Once the [configuration]
has been completed the integration tests can be run by executing the
`cassandra_integration_tests` executable. Integration tests are used to ensure
the functionality of the driver's API and/or the interaction of multiple
internal components.

### Running Tests Individually
Running the integration test executable without any arguments will start all
of the integration tests in sequence (one after another). The Boost Test
framework with which the integration and unit tests are compiled come with a
filter argument that allow for individual test execution. Using the
`--run_test` argument supplying the test suite and case will execute a single
test.

#### Example
The example below shows how to execute the `simple` test case in the `async`
test suite.

```bash
cassandra_integration_tests --run_test=async/simple
````

To run all of the test cases in the `async` suite wildcards can be used in
conjunction with the `run_test` argument

```bash
cassandra_integration_tests --run_test=async/*
```

[Cassandra Cluster Manager (CCM)]: http://datastax.github.io/cpp-driver/topics/testing/ccm/
[configuration]: http://datastax.github.io/cpp-driver/topics/testing/configuration/
