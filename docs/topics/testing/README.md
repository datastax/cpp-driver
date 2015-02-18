# Testing
Before proceeding ensure the tests were built using the build procedures found [here](http://datastax.github.io/cpp-driver/topics/building/#test-dependencies-and-building-tests-not-required)

Each test performs a [setup](#setup-cassandra), [execute](#execute-test), and
[teardown](#teardown-cassandra). This ensures that each test has a clean and
consistent run against the Apache Cassandra instance during the execution
phase.

## Execution Sequences
### Setup Cassandra
```ditaa
/------------------\               /------------\                  /-------------\                  /----------\
| Integration Test |               | CCM Bridge |                  | CCM Machine |                  | CCM Tool |
| cYEL             |               | cBLK       |                  | cBLU        |                  |cBLK      |
\---------+--------/               \------+-----/                  \-------+-----/                  \-----+----/
          :                               :                                :                              :
          :  Create and Start Cluster     :                                :                              :
         +++---------------------------->+++ Establish SSH Connection      :                              :
         | |                             | |----------------------------->+++                             :
         | |                             | |       Connection Established | |                             :
         | |                             | |<-----------------------------| |                             :
         | |                             | | Create N-Node Cluster        | |                             :
         | |                             | |----------------------------->| | Execute Create Cluster      :
         | |                             | |                              | |--------------------------->+++
         | |                             | |                              | |         Download Cassandra | |
         | |                             | |                              | |<---------------------------| |
         | |                             | |                              | |            Build Cassandra | |
         | |                             | |                              | |<---------------------------| |
         | |                             | |                              | |              Start Cluster | |
         | |                             | |                              | |<---------------------------+++
         | |                             | |      Cassandra Cluster Ready | |                             :
         | |     Cassandra Cluster is UP | |<-----------------------------+++                             :
         +++<----------------------------+++                               :                              :
          :                               :                                :                              :
          :                               :                                :                              :

```

### Execute Test
```ditaa
                /-----------\                                  /------------\
                | Unit Test |         Perform Test             | C++ Driver |
                | cYEL      +--------------------------------->| cBLU       |
                \-----+-----/                                  \------+-----/
                      ^                                               |
                      |                                               |
                      |             Validate Results                  |
                      +-----------------------------------------------+



   /------------\
   | C++ Driver |
/--+------------+--\                                                  /-------------\
| Integration Test |                   Perform Test                   | CCM Machine +------\
| cYEL             +------------------------------------------------->| cBLU        |NODE 1|
\--------+---------/                                                  |             +------/
         ^                                                            |             +------\
         |                                                            |             |NODE 2|
         |                           Validate Results                 |             +------/
         +-----------------------------------+------------------------+             +------\
                                             |                        |             |NODE 3|
                                             |                        |             +------/
                                             |                        \-------+-----/
                                             |                                |
                                             |                                |
                                             |                                |
                                   /---------+----------\                     |
                                   | Cassandra Cluster  |                     |
                                   |                    |     Perform Test    |
                                   |                    +<--------------------+
                                   |                    |
                                   | {s}                |
                                   | cGRE               |
                                   \--------------------/
```

### Teardown Cassandra
```ditaa
/------------------\               /------------\                  /-------------\                 /----------\
| Integration Test |               | CCM Bridge |                  | CCM Machine |                 | CCM Tool |
| cYEL             |               | cBLK       |                  | cBLU        |                 | cBLK     |
\---------+--------/               \------+-----/                  \-------+-----/                 \-----+----/
          :                               :                                :                             :
          :  Stop and Destroy Cluster     :                                :                             :
         +++---------------------------->+++ Establish SSH Connection      :                             :
         | |                             | |----------------------------->+++                            :
         | |                             | |       Connection Established | |                            :
         | |                             | |<-----------------------------| |                            :
         | |                             | | Destroy N-Node Cluster       | |                            :
         | |                             | |----------------------------->| | Remove Cluster             :
         | |                             | |                              | |-------------------------->+++
         | |                             | |                              | |  Stop Cassandra Instances | |
         | |                             | |                              | |<--------------------------| |
         | |                             | |                              | |           Destroy Cluster | |
         | |                             | |                              | |<--------------------------+++
         | |                             | |  Cassandra Cluster Destroyed | |                            :
         | | Cassandra Cluster Destrored | |<-----------------------------+++                            :
         +++<----------------------------+++                               :                             :
          :                               :                                :                             :
          :                               :                                :                             :

```

## TODO
Here are some of the items being scheduled for future enhancements.

- Incorporate integration tests into Jenkins environment
- Remove Boost Test Framework in Favor of Google Test Framework
- Updates to CCM Bridge
 - Add SSH key support (extra security over plain text password)
 - Simply configuration files
 - Allow files to be copied over SSH established connection
