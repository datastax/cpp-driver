# Building

The C/C++ DSE driver will build on most standard Unix-like and Microsoft
Windows platforms. Packages are available for the following platforms:

* [CentOS 6][cpp-dse-driver-centos6]
* [CentOS 7][cpp-dse-driver-centos7]
* [Ubuntu 14.04 LTS][cpp-dse-driver-ubuntu14-04]
* [Ubuntu 16.04 LTS][cpp-dse-driver-ubuntu16-04]
* [Ubuntu 18.04 LTS][cpp-dse-driver-ubuntu18-04]
* [Windows][cpp-dse-driver-windows]

__NOTE__: The build procedures only need to be performed for driver development
          or if your system doesn't have packages available for download and
          installation.

## Compatibility

* Architectures: 32-bit (x86) and 64-bit (x64)
* Compilers: GCC 4.1.2+ Clang 3.4+, and MSVC 2010/2012/2013/2015/2017/2019

## Dependencies

The C/C++ DSE driver depends on the following software:

* [CMake] v2.6.4+
* [libuv] 1.x
* Kerberos v5 ([Heimdal] or [MIT])
* [OpenSSL] v1.0.x or v1.1.x

## Linux/Mac OS

The driver is known to build on CentOS/RHEL 6/7, Mac OS X 10.10/10.11 (Yosemite
and El Capitan), Mac OS 10.12/10.13 (Sierra and High Sierra), and Ubuntu
14.04/16.04/18.04 LTS.

__NOTE__: The driver will also build on most standard Unix-like systems using
          GCC 4.1.2+ or Clang 3.4+.

### Installing dependencies

#### Initial environment setup

##### CentOS/RHEL (Yum)

```bash
yum install automake cmake gcc-c++ git libtool
```

##### Ubuntu (APT)

```bash
apt-get update
apt-get install build-essential cmake git
```

##### Mac OS (Brew)

[Homebrew][Homebrew] (or brew) is a free and open-source software package
management system that simplifies the installation of software on the Mac OS
operating system. Ensure [Homebrew is installed][Homebrew] before proceeding.

```bash
brew update
brew upgrade
brew install autoconf automake cmake libtool
```

#### Kerberos

##### CentOS/RHEL (Yum)

```bash
yum install krb5-devel
```

##### Ubuntu (APT)

```bash
apt-get install libkrb5-dev
```

#### libuv

libuv v1.x should be used in order to ensure all features of the C/C++ DSE
driver are available. When using a package manager for your operating system
make sure you install v1.x; if available.

##### CentOS/RHEL and Ubuntu packages

Packages are available from our [download server]:

* [CentOS 6][libuv-centos6]
* [CentOS 7][libuv-centos7]
* [Ubuntu 14.04 LTS][libuv-ubuntu14-04]
* [Ubuntu 16.04 LTS][libuv-ubuntu16-04]
* [Ubuntu 18.04 LTS][libuv-ubuntu18-04]

##### Mac OS (Brew)

```bash
brew install libuv
```

##### Manually build and install

_The following procedures should be performed if packages are not available for
your system._

```bash
pushd /tmp
wget http://dist.libuv.org/dist/v1.33.0/libuv-v1.33.0.tar.gz
tar xzf libuv-v1.33.0.tar.gz
pushd libuv-v1.33.0
sh autogen.sh
./configure
make install
popd
popd
```

#### OpenSSL

##### CentOS (Yum)

```bash
yum install openssl-devel
```

##### Ubuntu (APT)

```bash
apt-get install libssl-dev
```

##### Mac OS (Brew)

```bash
brew install openssl
```

__Note__: For Mac OS X 10.11 (El Capitan) and Mac OS 10.12/10.13 (Sierra and
          High Sierra) a link needs to be created in order to make OpenSSL
          available to the building libraries:

```bash
brew link --force openssl
```

##### Manually build and install

```bash
pushd /tmp
wget --no-check-certificate https://www.openssl.org/source/openssl-1.0.2s.tar.gz
tar xzf openssl-1.0.2s.tar.gz
pushd openssl-1.0.2s
CFLAGS=-fpic ./config shared
make
make install
popd
popd
```

### Building and installing the C/C++ DSE driver

```bash
mkdir build
pushd build
cmake ..
make
make install
popd
```

#### Building examples (optional)

Examples are not built by default and need to be enabled. Update your [CMake]
line to build examples.

```bash
cmake -DDSE_BUILD_EXAMPLES=On ..
```

#### Building tests (optional)

Tests (integration and unit) are not built by default and need to be enabled.

##### All tests

```bash
cmake -DDSE_BUILD_TESTS=On ..
```

__Note__: This will build both the integration and unit tests

##### Integration tests

```bash
cmake -DDSE_BUILD_INTEGRATION_TESTS=On ..
```

##### Unit tests

```bash
cmake -DDSE_BUILD_UNIT_TESTS=On ..
```

## Windows

The driver is known to build with Visual Studio 2010, 2012, 2013, 2015, 2017, and 2019.

### Obtaining build dependencies

* Download and install [Bison]
  * Make sure Bison is in your system PATH and not installed in a directory with
    spaces (e.g. `%SYSTEMDRIVE%\GnuWin32`)
* Download and install [CMake]
  * Make sure to select the option "Add CMake to the system PATH for all users"
    or "Add CMake to the system PATH for current user"
* Download and install [Strawberry Perl] or [ActiveState Perl]
  * Make sure to select the option "Add Perl to PATH environment variable"
* Download and install Kerberos for Windows v4.0.1
  * [32-bit][k4w-32]
  * [64-bit][k4w-64]

### Building the driver

First you will need to open a "Command Prompt" to execute the CMake commands.

#### Building the C/C++ DSE driver

Supported generators are:
* Visual Studio 10 2010
* Visual Studio 11 2012
* Visual Studio 12 2013
* Visual Studio 14 2015
* Visual Studio 15 2017
* Visual Studio 16 2019

```bash
mkdir build
pushd build
cmake -G "Visual Studio 16 2019" -A x64 ..
cmake --build .
popd
```

__Note__: To build 32-bit binaries/libraries use `-A Win32`.

#### Building examples (optional)

Examples are not built by default and need to be enabled. Update your [CMake]
line to build examples.

```bash
cmake -G "Visual Studio 16 2019" -A x64 -DDSE_BUILD_EXAMPLES=On ..
```

#### Building tests (optional)

Tests (integration and unit) are not built by default and need to be enabled.

##### All tests

```bash
cmake -G "Visual Studio 16 2019" -A x64 -DDSE_BUILD_TESTS=On ..
```

__Note__: This will build both the integration and unit tests


##### Integration tests

```bash
cmake -G "Visual Studio 16 2019" -A x64 -DDSE_BUILD_INTEGRATION_TESTS=On ..
```

##### Unit tests

```bash
cmake -G "Visual Studio 16 2019" -A x64 -DDSE_BUILD_UNIT_TESTS=On ..
```

[download server]: http://downloads.datastax.com
[cpp-dse-driver-centos6]: http://downloads.datastax.com/cpp-driver/centos/6/dse
[cpp-dse-driver-centos7]: http://downloads.datastax.com/cpp-driver/centos/7/dse
[cpp-dse-driver-ubuntu14-04]: http://downloads.datastax.com/cpp-driver/ubuntu/14.04/dse
[cpp-dse-driver-ubuntu16-04]: http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dse
[cpp-dse-driver-ubuntu18-04]: http://downloads.datastax.com/cpp-driver/ubuntu/18.04/dse
[cpp-dse-driver-windows]: http://downloads.datastax.com/cpp-driver/windows/dse
[libuv-centos6]: http://downloads.datastax.com/cpp-driver/centos/6/dependencies/libuv
[libuv-centos7]: http://downloads.datastax.com/cpp-driver/centos/7/dependencies/libuv
[libuv-ubuntu14-04]: http://downloads.datastax.com/cpp-driver/ubuntu/14.04/dependencies/libuv
[libuv-ubuntu16-04]: http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dependencies/libuv
[libuv-ubuntu18-04]: http://downloads.datastax.com/cpp-driver/ubuntu/18.04/dependencies/libuv
[Homebrew]: https://brew.sh
[Bison]: http://gnuwin32.sourceforge.net/downlinks/bison.php
[CMake]: http://www.cmake.org/download
[Strawberry Perl]: http://strawberryperl.com
[ActiveState Perl]: https://www.perl.org/get.html#win32
[k4w-32]: http://web.mit.edu/kerberos/dist/kfw/4.0/kfw-4.0.1-i386.msi
[k4w-64]: http://web.mit.edu/kerberos/dist/kfw/4.0/kfw-4.0.1-amd64.msi
[libuv]: http://libuv.org
[Heimdal]: https://www.h5l.org
[MIT]: https://web.mit.edu/kerberos
[OpenSSL]: https://www.openssl.org
