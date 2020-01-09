# Installation

## Packages

Pre-built packages are available for CentOS 6/7, Ubuntu 14.04/16.04/18.04 and
Windows 7 SP1 and above.

### CentOS

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
   <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>CentOS 6</td>
   <td>http://downloads.datastax.com/cpp-driver/centos/6/cassandra</td>
  </tr>
  <tr>
   <td>CentOS 7</td>
   <td>http://downloads.datastax.com/cpp-driver/centos/7/cassandra</td>
  </tr>
  </tbody>
</table>

#### Dependencies

CentOS doesn't have up-to-date versions of libuv so we provide current packages.

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
   <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>CentOS 6</td>
   <td>http://downloads.datastax.com/cpp-driver/centos/6/dependencies</td>
  </tr>
  <tr>
   <td>CentOS 7</td>
   <td>http://downloads.datastax.com/cpp-driver/centos/7/dependencies</td>
  </tr>
  </tbody>
</table>

#### To Install

Install dependencies:

```bash
yum install openssl krb5 zlib
rpm -Uvh libuv-<version>.rpm
```

Note: Replace `<version>` with the release version of the package.

Install the runtime library:

```bash
rpm -Uvh cassandra-cpp-driver-<version>.rpm
```

When developing against the driver you'll also want to install the development
package and the debug symbols.

```bash
rpm -Uvh cassandra-cpp-driver-devel-<version>.rpm
rpm -Uvh cassandra-cpp-driver-debuginfo-<version>.rpm
```

### Ubuntu

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
   <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>Ubuntu 14.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/14.04/cassandra</td>
  </tr>
  <tr>
   <td>Ubuntu 16.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra</td>
  </tr>
  <tr>
   <td>Ubuntu 18.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/18.04/cassandra</td>
  </tr>
  </tbody>
</table>

#### Dependencies

Ubuntu doesn't have up-to-date versions of libuv so we provide current packages.

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
   <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>Ubuntu 14.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/14.04/dependencies</td>
  </tr>
  <tr>
   <td>Ubuntu 16.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dependencies</td>
  </tr>
  <tr>
   <td>Ubuntu 18.04</td>
   <td>http://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies</td>
  </tr>
  </tbody>
</table>

#### To Install

Install dependencies:

```bash
apt-get install libssl libkrb5 zlib1g
dpkg -i libuv_<version>.deb
```

Note: Replace `<version>` with the release version of the package.

Install the runtime library:

```bash
dpkg -i cassandra-cpp-driver_<version>.deb
```

When developing against the driver you'll also want to install the development
package and the debug symbols.

```bash
dpkg -i cassandra-cpp-driver-dev_<version>.deb
dpkg -i cassandra-cpp-driver-dbg_<version>.deb
```

### Windows

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
    <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>Windows</td>
   <td>http://downloads.datastax.com/cpp-driver/windows/cassandra</td>
  </tr>
  </tbody>
</table>

#### Dependencies

We provide packages (`.zip` files) for all the dependencies (except for
Kerberos) on Windows because they can be difficult to install/build.

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Version</th>
   <th>URL</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td>Windows</td>
   <td>http://downloads.datastax.com/cpp-driver/windows/dependencies</td>
  </tr>
  </tbody>
</table>

#### To Install

First, you will need to download and install [Kerberos] for Windows.

Unzip the packages (from http://downloads.datastax.com) and add the include and
library directories to your project's `Additional Include Directories` and
`Additional Dependencies` configuration properties.

## Building

If pre-built packages are not available for your platform or architecture you
will need to build the driver from source. Directions for building and
installing the DataStax C/C++ Driver for Apache Cassandra and DataStax Products
can be found [here](/topics/building/).

[Kerberos]: https://web.mit.edu/kerberos
