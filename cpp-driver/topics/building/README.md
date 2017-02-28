# Building

## Supported Platforms
The driver is known to work on CentOS/RHEL 5/6/7, Mac OS X 10.8/10.9/10.10/10.11
(Moutain Lion, Mavericks, Yosemite, and El Capitan), Ubuntu 12.04/14.04 LTS, and
Windows 7 SP1 and above.

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2012/2013/2015.

## Dependencies

### Driver

- [CMake](http://www.cmake.org)
- [libuv (1.x or 0.10.x)](https://github.com/libuv/libuv)
- [OpenSSL](http://www.openssl.org/) (optional)

**NOTE:** Utilizing the default package manager configuration to install
dependencies on \*nix based operating systems may result in older versions of
dependencies being installed.

### Test Dependencies

- [boost 1.59+](http://www.boost.org)
- [libssh2](http://www.libssh2.org) (optional)

## Linux/OS X
The driver has been built using both Clang (Ubuntu 12.04/14.04 and OS X) and GCC
(Linux).

### Obtaining Dependencies

#### CentOS/RHEL

##### Additional Requirements for CentOS/RHEL 5
CentOS/RHEL 5 does not contain `git` in its core repositories; however, the [EPEL] has
this dependency.

```bash
sudo yum -y install epel-release
```

##### Dependencies and libuv Installation

```bash
sudo yum install automake cmake gcc-c++ git libtool openssl-devel wget
pushd /tmp
wget http://dist.libuv.org/dist/v1.11.0/libuv-v1.11.0.tar.gz
tar xzf libuv-v1.11.0.tar.gz
pushd libuv-v1.11.0
sh autogen.sh
./configure
sudo make install
popd
popd
```

#### OS X
The driver has been built and tested using the Clang compiler provided by XCode
5.1+. The dependencies were obtained using [Homebrew](http://brew.sh).

```bash
brew install libuv cmake
```

**NOTE:** The driver utilizes the OpenSSL library included with XCode unless
          running XCode 7+.

##### Additional Requirements for 10.11+ (El Capitan) and XCode 7+
OpenSSL has been officially removed from the OS X SDK 10.11+ and requires
additional configuration before building the driver.

```bash
brew install openssl
brew link --force openssl
```

#### Ubuntu

##### Additional Requirements for Ubuntu 12.04
Ubuntu 12.04 does not contain libuv in its repositories; however the LinuxJedi
PPA has a backport from Ubuntu 14.04 which can be found
[here](https://launchpad.net/~linuxjedi/+archive/ubuntu/ppa).

```bash
sudo apt-add-repository ppa:linuxjedi/ppa
sudo apt-get update
```

##### GCC

```bash
sudo apt-get install g++ make cmake libuv-dev libssl-dev
```

##### Clang

```bash
sudo apt-get install clang make cmake libuv-dev libssl-dev
```

### Building the Driver

```bash
git clone https://github.com/datastax/cpp-driver.git
mkdir cpp-driver/build
cd cpp-driver/build
cmake ..
make
```

### Test Dependencies and Building the Tests (_NOT REQUIRED_)

#### Obtaining Test Dependencies

##### CentOS/RHEL
CentOS/RHEL does not contain Boost v1.59+ libraries in its repositories; however
these can be easily installed from source. Ensure previous version of Boost has
been removed by executing the command `sudo yum remove boost*` before
proceeding.

```bash
sudo yum install libssh2-devel
pushd /tmp
wget http://sourceforge.net/projects/boost/files/boost/1.63.0/boost_1_63_0.tar.gz/download -O boost_1_63_0.tar.gz
tar xzf boost_1_63_0.tar.gz
pushd boost_1_63_0
./bootstrap.sh --with-libraries=atomic,chrono,system,thread,test
sudo ./b2 cxxflags="-fPIC" install
popd
popd
```

**NOTE:** CentOS/RHEL 5 has known issues when compiling tests with GCC 4.1.2 as
it is not a supported Boost compiler.

##### OS X

```bash
brew install boost libssh2
```

##### Ubuntu
Ubuntu does not contain Boost v1.59+ libraries in its repositories; however
these can be easily installed from source.

```bash
sudo apt-get install libssh2-1-dev
pushd /tmp
wget http://sourceforge.net/projects/boost/files/boost/1.63.0/boost_1_63_0.tar.gz/download -O boost_1_63_0.tar.gz
tar xzf boost_1_63_0.tar.gz
pushd boost_1_63_0
./bootstrap.sh --with-libraries=atomic,chrono,system,thread,test
sudo ./b2 install
popd
popd
```

#### Building the Driver with the Tests

```bash
git clone https://github.com/datastax/cpp-driver.git
mkdir cpp-driver/build
cd cpp-driver/build
cmake -DCASS_BUILD_TESTS=ON ..
make
```

## Windows
The driver has been built and tested using Microsoft Visual Studio 2010, 2012,
2013 and 2015 (using the "Express" and Professional versions) and Windows SDK
v7.1, 8.0, 8.1, and 10.0 on Windows 7 SP1 and above. The library dependencies
will automatically download and build; however the following build dependencies
will need to be installed.

### Obtaining Build Dependencies
- Download and install [CMake](http://www.cmake.org/download).
 - Make sure to select the option "Add CMake to the system PATH for all users"
   or "Add CMake to the system PATH for current user".
- Download and install [Git](http://git-scm.com/download/win)
 - Make sure to select the option "Use Git from Windows Command Prompt" or
   manually add the git executable to the system PATH.
- Download and install [ActiveState Perl](https://www.perl.org/get.html#win32)
 - Make sure to select the option "Add Perl to PATH environment variable".
 - **NOTE:** This build dependency is required if building with OpenSSL support
- Download and install [Python v2.7.x](https://www.python.org/downloads)
 - Make sure to select/install the feature "Add python.exe to Path"

### Building the Driver
A batch script has been created to detect installed versions of Visual Studio
(and/or Windows SDK installations) to simplify the build process on Windows. If
you have more than one version of Visual Studio (and/or Windows SDK) installed
you will be prompted to select which version to use when compiling the driver.

First you will need to open a "Command Prompt" (or Windows SDK Command Prompt)
to execute the batch script.

```dos
Usage: VC_BUILD.BAT [OPTION...]

    --DEBUG                           Enable debug build
    --RELEASE                         Enable release build (default)
    --DISABLE-CLEAN                   Disable clean build
    --DEPENDENCIES-ONLY               Build dependencies only
    --TARGET-COMPILER [version]       140, 120, 110, 100, or WINSDK
    --DISABLE-OPENSSL                 Disable OpenSSL support
    --ENABLE-EXAMPLES                 Enable example builds
    --ENABLE-PACKAGES [version]       Enable package generation (*)
    --ENABLE-ZLIB                     Enable zlib
    --GENERATE-SOLUTION               Generate Visual Studio solution (**)
    --INSTALL-DIR [install-dir]       Override installation directory
    --SHARED                          Build shared library (default)
    --STATIC                          Build static library
    --X86                             Target 32-bit build (***)
    --X64                             Target 64-bit build (***)
    --USE-BOOST-ATOMIC                Use Boost atomic

    Testing Arguments

    --ENABLE-TESTS
         [boost-root-dir]             Enable test builds
    --ENABLE-INTEGRATION-TESTS
         [boost-root-dir]             Enable integration tests build
    --ENABLE-UNIT-TESTS
         [boost-root-dir]             Enable unit tests build
    --ENABLE-LIBSSH2                  Enable libssh2 (remote server testing)

    --HELP                            Display this message

*   Packages are only generated using detected installations of Visual Studio
**  Dependencies are built before generation of Visual Studio solution
*** Default target architecture is determined based on system architecture
```

To build 32-bit shared library:

```dos
VC_BUILD.BAT --X86
```

To build 64-bit shared library:

```dos
VC_BUILD.BAT --X64
```

To build using Boost atomic implementation:

```dos
VC_BUILD.BAT --USE-BOOST-ATOMIC
```

To build static library:

```dos
VC_BUILD.BAT --STATIC
```

To build examples:

```dos
VC_BUILD.BAT --ENABLE-EXAMPLES
```

To build library without OpenSSL support:

```dos
VC_BUILD.BAT --DISABLE-OPENSSL
```

To build 32-bit static library without OpenSSL support:

```dos
VC_BUILD.BAT --DISABLE-OPENSSL --STATIC --X86
```

To generate Visual Studio solution file:

```dos
VC_BUILD.BAT --GENERATE-SOLUTION
```

To use vc_build.bat for easy inclusion into a project:

```dos
VC_BUILD.BAT --TARGET-COMPILER 120 --INSTALL-DIR C:\myproject\dependencies\libs\cpp-driver
```

**NOTE:** When overriding installation directory using `--INSTALL-DIR`, the
driver dependencies will also be copied (e.g. C:\myproject\dependencies\libs)

### Test Dependencies and Building the Tests (_NOT REQUIRED_)

#### Obtaining Test Dependencies
Boost v1.59+ is the only external dependency that will need to be obtained in
order to build the unit and integration tests.

To simplify the process; pre-built binaries can be obtained
[here](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/).
Ensure the proper Visual Studio (or Windows SDK) version and architecture is
obtained and select from the following list:

- Visual Studio 2010 (Windows SDK 7.1)
 - Boost v1.63.0 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-10.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-10.0-64.exe/download)
- Visual Studio 2012 (Windows SDK 8.0)
 - Boost v1.63.0 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-11.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-11.0-64.exe/download)
- Visual Studio 2013 (Windows SDK 8.1)
 - Boost v1.63.0 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-12.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-12.0-64.exe/download)
- Visual Studio 2015 (Windows SDK 10.0)
 - Boost v1.63.0 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-14.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.63.0/boost_1_63_0-msvc-14.0-64.exe/download)

#### Building the Driver with the Tests

```dos
VC_BUILD.BAT --STATIC --ENABLE-TESTS <ABSOLUTE-PATH-TO-BOOST>

[e.g. C:\local\boost_1_63_0]
```
 **NOTE:** When enabling tests, --USE-BOOST-ATOMIC will use the Boost atomic
 implementation supplied by <ABSOLUTE-PATH-TO-BOOST>

[EPEL]: https://fedoraproject.org/wiki/EPEL
