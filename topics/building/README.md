# Building

The C/C++ driver will build on most standard Unix-like and Microsoft Windows
platforms. Packages are available for the following platforms:

* [CentOS 6][cpp-driver-centos6]
* [CentOS 7][cpp-driver-centos7]
* [Ubuntu 14.04 LTS][cpp-driver-ubuntu14-04]
* [Ubuntu 16.04 LTS][cpp-driver-ubuntu16-04]
* [Windows][cpp-driver-windows]

__NOTE__: The build procedures only need to be performed for driver development
          or if your system doesn't have packages available for download and
          installation.

## Compatibility

* Architectures: 32-bit (x86) and 64-bit (x64)
* Compilers: GCC 4.1.2+ Clang 3.4+, and MSVC 2010/2012/2013/2015/2017

## Dependencies

The C/C++ driver depends on the following software:

* [CMake] v2.6.4+
* [libuv] 1.x
* [OpenSSL] v1.0.x or v1.1.x

## Linux/Mac OS

The driver is known to build on CentOS/RHEL 6/7, Mac OS X 10.10/10.11 (Yosemite
and El Capitan), Mac OS 10.12 (Sierra), and Ubuntu 14.04/16.04 LTS.

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

#### libuv

libuv v1.x should be used in order to ensure all features of the C/C++ driver
are available. When using a package manager for your operating system make sure
you install v1.x; if available.

##### CentOS/RHEL and Ubuntu packages

Packages are available from our [download server]:

* [CentOS 6][libuv-centos6]
* [CentOS 7][libuv-centos7]
* [Ubuntu 14.04 LTS][libuv-ubuntu14-04]
* [Ubuntu 16.04 LTS][libuv-ubuntu16-04]

##### Mac OS (Brew)

```bash
brew install libuv
```

##### Manually build and install

_The following procedures should be performed if packages are not available for
your system._

```bash
pushd /tmp
wget http://dist.libuv.org/dist/v1.14.0/libuv-v1.14.0.tar.gz
tar xzf libuv-v1.14.0.tar.gz
pushd libuv-v1.14.0
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

__Note__: For Mac OS X 10.11 (El Capitan) and Mac OS 10.12 (Sierra) a link
          needs to be created in order to make OpenSSL available to the
          building libraries:

```bash
brew link --force openssl
```

##### Manually build and install

```bash
pushd /tmp
wget --no-check-certificate https://www.openssl.org/source/openssl-1.0.2l.tar.gz
tar xzf openssl-1.0.2l.tar.gz
pushd openssl-1.0.2l
CFLAGS=-fpic ./config shared
make
make install
popd
popd
```

### Building and installing the C/C++ driver

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
cmake -DCASS_BUILD_EXAMPLES=On ..
```

#### Building tests (optional)

Tests (integration and unit) are not built by default and need to be enabled.
Before proceeding [Boost] v1.59.0+ must be installed to properly configure and
build the integration and unit tests.

Once [Boost] is installed, update your CMake line to build tests.

##### Boost

###### CentOS/RHEL and Ubuntu

CentOS/RHEL and Ubuntu do not contain Boost v1.59+ libraries in its
repositories; however the following steps will install Boost from source:

__Note__: Ensure previous version of Boost has been removed before proceeding.

```bash
pushd /tmp
wget http://sourceforge.net/projects/boost/files/boost/1.64.0/boost_1_64_0.tar.gz/download -O boost_1_64_0.tar.gz
tar xzf boost_1_64_0.tar.gz
pushd boost_1_64_0
./bootstrap.sh --with-libraries=atomic,chrono,system,thread,test
sudo ./b2 cxxflags="-fPIC" install
popd
popd
```

###### Mac OS

```bash
brew install boost
```

##### Integration tests

```bash
cmake -DCASS_BUILD_INTEGRATION_TESTS=On ..
```

##### Unit tests

```bash
cmake -DCASS_BUILD_UNIT_TESTS=On ..
```


## Windows

We provide a self-contained [batch script] for building the C/C++ driver and
all of its dependencies. In order to run it, you have to install the build
dependencies and clone the repository with the DataStax C/C++ driver for
DataStax Enterprise.

### Obtaining build dependencies

* Download and install [Bison]
  * Make sure Bison is in your system PATH and not installed in a directory with
    spaces (e.g. `%SYSTEMDRIVE%\GnuWin32`)
* Download and install [CMake]
  * Make sure to select the option "Add CMake to the system PATH for all users"
    or "Add CMake to the system PATH for current user"
* Download and install [Git]
  * Make sure to select the option "Use Git from Windows Command Prompt" or
    manually add the git executable to the system PATH.
* Download and install [ActiveState Perl]
  * Make sure to select the option "Add Perl to PATH environment variable"
* Download and install [Python v2.7.x][python-27]
  * Make sure to select/install the feature "Add python.exe to Path"
* Download and install [Boost][Boost] (Optional: Tests only)
  * Visual Studio 2010: [32-bit][boost-msvc-100-32-bit]/[64-bit][boost-msvc-100-64-bit]
  * Visual Studio 2012: [32-bit][boost-msvc-110-32-bit]/[64-bit][boost-msvc-110-64-bit]
  * Visual Studio 2013: [32-bit][boost-msvc-120-32-bit]/[64-bit][boost-msvc-120-64-bit]
  * Visual Studio 2015: [32-bit][boost-msvc-140-32-bit]/[64-bit][boost-msvc-140-64-bit]
  * Visual Studio 2017: [32-bit][boost-msvc-141-32-bit]/[64-bit][boost-msvc-141-64-bit]

### Building the driver

The [batch script] detects installed versions of Visual Studio to simplify the
build process on Windows and select the correct version of Visual Studio when
compiling the driver.

First you will need to open a "Command Prompt" to execute the batch script.
Running the batch script without any arguments will build the driver for C/C++
driver for the current system architecture (e.g. x64).

To perform advanced build configuration, execute the batch script with the
`--HELP` argument to display the options available.

```dos
Usage: VC_BUILD.BAT [OPTION...]

    --DEBUG                           Enable debug build
    --RELEASE                         Enable release build
    --RELEASE-WITH-DEBUG-INFO         Enable release w/ dbginfo build (default)
    --DISABLE-CLEAN                   Disable clean build
    --DEPENDENCIES-ONLY               Build dependencies only
    --OPENSSL-VERSION                 OpenSSL version 1.0, 1.1 (default: 1.0)
    --TARGET-COMPILER [version]       141, 140, 120, 110, or 100
    --DISABLE-OPENSSL                 Disable OpenSSL support
    --ENABLE-EXAMPLES                 Enable example builds
    --ENABLE-PACKAGES [version]       Enable package generation (*)
    --ENABLE-ZLIB                     Enable zlib
    --GENERATE-SOLUTION               Generate Visual Studio solution (**)
    --INSTALL-DIR [install-dir]       Override installation directory
    --SHARED                          Build shared library (default)
    --STATIC                          Build static library
    --ENABLE-SHARED-OPENSSL           Force shared OpenSSL library
    --X86                             Target 32-bit build (***)
    --X64                             Target 64-bit build (***)
    --USE-BOOST-ATOMIC                Use Boost atomic

    Testing Arguments

    --ENABLE-TESTS
         [boost-root-dir]             Enable integration and unit tests build
    --ENABLE-INTEGRATION-TESTS
         [boost-root-dir]             Enable integration tests build
    --ENABLE-UNIT-TESTS
         [boost-root-dir]             Enable unit tests build
    --ENABLE-LIBSSH2                  Enable libssh2 (remote server testing)

    --HELP                            Display this message

*   Packages are only generated using detected installations of Visual Studio
    NOTE: OpenSSL v1.0.x is used for all package builds
**  Dependencies are built before generation of Visual Studio solution
*** Default target architecture is determined based on system architecture
```

__Note__: When overriding installation directory using `--INSTALL-DIR`, the
          driver dependencies will also be copied (e.g.
          C:\myproject\dependencies\libs)

[download server]: http://downloads.datastax.com
[cpp-driver-centos6]: http://downloads.datastax.com/cpp-driver/centos/6/cassandra
[cpp-driver-centos7]: http://downloads.datastax.com/cpp-driver/centos/7/cassandra
[cpp-driver-ubuntu14-04]: http://downloads.datastax.com/cpp-driver/ubuntu/14.04/cassandra
[cpp-driver-ubuntu16-04]: http://downloads.datastax.com/cpp-driver/ubuntu/16.04/cassandra
[cpp-driver-windows]: http://downloads.datastax.com/cpp-driver/windows/cassandra
[libuv-centos6]: http://downloads.datastax.com/cpp-driver/centos/6/dependencies/libuv
[libuv-centos7]: http://downloads.datastax.com/cpp-driver/centos/7/dependencies/libuv
[libuv-ubuntu14-04]: http://downloads.datastax.com/cpp-driver/ubuntu/14.04/dependencies/libuv
[libuv-ubuntu16-04]: http://downloads.datastax.com/cpp-driver/ubuntu/16.04/dependencies/libuv
[batch script]: ../../vc_build.bat
[Homebrew]: https://brew.sh
[Bison]: http://gnuwin32.sourceforge.net/downlinks/bison.php
[CMake]: http://www.cmake.org/download
[Git]: http://git-scm.com/download/win
[ActiveState Perl]: https://www.perl.org/get.html#win32
[python-27]: https://www.python.org/downloads
[libuv]: http://libuv.org
[OpenSSL]: https://www.openssl.org
[Boost]: http://www.boost.org
[boost-msvc-100-32-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-10.0-32.exe/download
[boost-msvc-100-64-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-10.0-64.exe/download
[boost-msvc-110-32-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-11.0-32.exe/download
[boost-msvc-110-64-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-11.0-64.exe/download
[boost-msvc-120-32-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-12.0-32.exe/download
[boost-msvc-120-64-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-12.0-64.exe/download
[boost-msvc-140-32-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-14.0-32.exe/download
[boost-msvc-140-64-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-14.0-64.exe/download
[boost-msvc-141-32-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-14.1-32.exe/download
[boost-msvc-141-64-bit]: http://sourceforge.net/projects/boost/files/boost-binaries/1.64.0/boost_1_64_0-msvc-14.1-64.exe/download
