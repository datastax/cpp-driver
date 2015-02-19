# Configuration
In order to execute the integration tests, a `config.txt` file must be
created in the working directory where the integration tests are run. Below is
an example of the configuration file for use with the [CCM Cluster] VM.

```text
##
# CCM Options
##
# CCM Cassandra Deployment Version
#
# Cassandra version to deploy using CCM
# (branches and tags can be used from https://github.com/apache/cassandra)
#
CASSANDRA_VERSION=2.1.3

##
# SSH Options
# Enusre DEPLOYMENT_TYPE=ssh to enable these options
##
# SSH Host (Hostname/IP Address)
#
# Host/IP to use when establishing ssh connection for remote CCM command
# execution
#
SSH_HOST=192.168.33.11
#
# SSH Port (Port)
#
# TCP/IP port to use when establishing ssh connection for remote CCM command
# execution
#
SSH_PORT=22
#
# SSH Username (Username)
#
# Username for authenticating SSH connection
#
SSH_USERNAME=vagrant
#
# SSH Password (Password)
#
# Password for authenticating SSH connection
#
# NOTE: This password is plain text
#
SSH_PASSWORD=vagrant

##
# CPP-Driver Options
##
#
# IP Prefix (Single/Multiple Node Test Connections)
#
# Cassandra node IP prefix for cpp-driver connection(s) and CCM node creation
#
IP_PREFIX=192.168.33.1
```
(where `CASSANDRA_VERSION=2.1.3` is the latest version of Cassandra)

## Driver Downgrade Test Suite
To execute any of the tests in the `version1_downgrade` test suite, a copy of
`config.txt` above should be created called `config_v1.txt` and the
`CASSANDRA_VERSION` option must be updated to a Cassandra in the 1.x family.

```text
CASSANDRA_VERSION=1.2.19
```
(where `CASSANDRA_VERSION=1.2.19` is the latest version of Cassandra 1.2)

## SSL Test Suite
To execute any of the tests in the `ssl` test suite, follow the [instructions]
for generating public and private keys to use with the driver and Cassandra.

[CCM Cluster]: http://datastax.github.io/cpp-driver/topics/testing/ccm/#ccm-cluster-by-way-of-vagrant-and-virtual-box
[instructions]: https://github.com/datastax/cpp-driver/tree/1.0/test/ccm_bridge/data/ssl
