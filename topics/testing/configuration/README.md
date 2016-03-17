# Configuration
In order to execute the integration tests, a `config.txt` file must be
created in the working directory where the integration tests are run. Below is
an example of the configuration file for use with the [CCM Cluster] VM.

```text
###############################################################################
# This is configuration file for CCM bridge library used primarily by the     #
# integration testing framework                                               #
###############################################################################

###############################################################################
# CCM Options                                                                 #
###############################################################################
##
# CCM Cassandra Deployment Version
#
# Cassandra version to deploy using CCM
#
# Uncomment to specify Cassandra version
##
#CASSANDRA_VERSION=3.4
##
# Flag to determine if Cassandra/DSE version should be obtained from ASF/GitHub
#
# Uncomment to specify use of ASF/GitHub download
##
#USE_GIT=false
##
# CCM Deployment Type (local|remote)
#
# Setting to indicate how CCM commands should be run (locally or through SSH)
#
# Uncomment to specify deployment type (defaults to local)
##
#DEPLOYMENT_TYPE=local

###############################################################################
# DSE Options                                                                 #
# Ensure USE_DSE=true to enable these options                                 #
###############################################################################
##
# Flag to determine if DSE version should be loaded
#
# Uncomment to specify use of DSE
##
#USE_DSE=false
##
# DSE Deployment Version
#
# DSE version to deploy using CCM
#
# Uncomment to specify DSE version
##
#DSE_VERSION=4.8.5
##
# CCM DSE Credentials Type (username_password|ini_file)
#
# Setting to indicate how DSE download through CCM should authenticate access
#
# Uncomment to specify DSE credentials type (defaults to username and password)
##
#DSE_CREDENTIALS_TYPE=username_password
##
# DSE Username (Username)
#
# Username for authenticating DSE download access
#
# Uncomment to specify DSE download access username
##
#DSE_USERNAME=
##
# DSE Password (Password)
#
# Password for authenticating DSE download access
#
# NOTE: This password is plain text
#
# Uncomment to specify DSE download access password
##
#DSE_PASSWORD=

###############################################################################
# SSH Options                                                                 #
# Enusre DEPLOYMENT_TYPE=remote to enable these options                       #
###############################################################################
##
# SSH Port (Port)
#
# TCP/IP port to use when establishing ssh connection for remote CCM command
# execution
#
# Uncomment to specify SSH port (defaults to 22)
##
#SSH_PORT=22
##
# CCM Authentication Type (username_password|public_key)
#
# Setting to indicate how SSH connections should be authenticated
#
# Uncomment to specify authentication type (defaults to username and password)
##
#AUTHENTICATION_TYPE=username_password
##
# SSH Username (Username)
#
# Username for authenticating SSH connection
#
# Uncomment to specify SSH username (defaults to vagrant)
##
#SSH_USERNAME=vagrant
##
# SSH Password (Password)
#
# Password for authenticating SSH connection
#
# NOTE: This password is plain text
#
# Uncomment to specify SSH password (defaults to vagrant)
##
#SSH_PASSWORD=vagrant
##
# SSH Public Key
#
# Path name of the public key file
#
# Uncomment to specify public key filename
##
#SSH_PUBLIC_KEY=
##
# SSH Private Key
#
# Path name of the private key file
#
# Uncomment to specify private key filename
##
#SSH_PRIVATE_KEY=

###############################################################################
# C/C++-Driver Options                                                        #
###############################################################################
##
# Host (Single/Multiple Node Test Connections)
#
# Cassandra node IP for cpp-driver connection(s), CCM node creation, and SSH
# remote connections
#
# NOTE: This must be in IPv4 format
#
# Uncomment to specify host/ip address
##
#HOST=127.0.0.1
```

## SSL Test Suite
To execute any of the tests in the `ssl` test suite, follow the [instructions]
for generating public and private keys to use with the driver and Cassandra.

[CCM Cluster]: http://datastax.github.io/cpp-driver/topics/testing/ccm/#ccm-cluster-by-way-of-vagrant-and-virtual-box
[instructions]: https://github.com/datastax/cpp-driver/tree/1.0/test/ccm_bridge/data/ssl
