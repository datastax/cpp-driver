# Building

## Supported Platforms
The driver is known to work on CentOS/RHEL 6/7, Mac OS X 10.8/10.9/10.10/10.11
(Moutain Lion, Mavericks, Yosemite, and El Capitan), Ubuntu 12.04/14.04 LTS, and
Windows 7 SP1 and above.

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2012/2013/2015.

## Dependencies

### Driver

- [CMake](http://www.cmake.org)
- [libuv (1.x or 0.10.x)](https://github.com/libuv/libuv)
- [Kerberos](http://web.mit.edu/kerberos/)
- [OpenSSL](http://www.openssl.org/) (optional)

**NOTE:** Utilizing the default package manager configuration to install
dependencies on \*nix based operating systems may result in older versions of
dependencies being installed.

#### Kerberos

In addition to the dependencies required by the core driver the DSE driver also
requires Kerberos. Kerberos can be obtains by using these packages:

* Ubuntu requires the package `libkrb5-dev`
* RHEL (CentOS) requires the package `krb5-devel`
* Windows requires [Kerberos for Windows] to be installed

### Test Dependencies

- [libssh2](http://www.libssh2.org) (optional)

## Linux/OS X
The driver has been built using both Clang (Ubuntu 12.04/14.04 and OS X) and GCC
(Linux).

### Obtaining Dependencies

#### CentOS/RHEL

##### Dependencies and libuv Installation

```bash
sudo yum install automake cmake gcc-c++ git krb5-devel libtool openssl-devel wget
pushd /tmp
wget http://dist.libuv.org/dist/v1.8.0/libuv-v1.8.0.tar.gz
tar xzf libuv-v1.8.0.tar.gz
pushd libuv-v1.8.0
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
sudo apt-get install g++ make cmake libuv-dev libkrb5-dev libssl-dev
```

##### Clang

```bash
sudo apt-get install clang make cmake libuv-dev libkrb5-dev libssl-dev
```

### Building the Driver

```bash
git clone https://github.com/datastax/cpp-driver-dse.git
mkdir cpp-driver-dse/build
cd cpp-driver-dse/build
cmake ..
make
```

### Test Dependencies and Building the Tests (_NOT REQUIRED_)

#### Obtaining Test Dependencies

##### CentOS/RHEL

```bash
sudo yum install libssh2-devel
```

##### OS X

```bash
brew install libssh2
```

##### Ubuntu

```bash
sudo apt-get install libssh2-1-dev
```

#### Building the Driver with the Tests

```bash
git clone https://github.com/datastax/cpp-driver-dse.git
mkdir cpp-driver-dse/build
cd cpp-driver-dse/build
cmake -DDSE_BUILD_TESTS=ON ..
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
- Download and install Kerberos for Windows v4.0.1
 - [32-bit](http://web.mit.edu/kerberos/dist/kfw/4.0/kfw-4.0.1-i386.msi)
 - [64-bit](http://web.mit.edu/kerberos/dist/kfw/4.0/kfw-4.0.1-amd64.msi)

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

    --ENABLE-INTEGRATION-TESTS        Enable integration tests build
    --ENABLE-UNIT-TESTS               Enable unit tests build
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

To generate Visual Studio solution file:

```dos
VC_BUILD.BAT --GENERATE-SOLUTION
```

To use vc_build.bat for easy inclusion into a project:

```dos
VC_BUILD.BAT --TARGET-COMPILER 120 --INSTALL-DIR C:\myproject\dependencies\libs\cpp-driver-dse
```

**NOTE:** When overriding installation directory using `--INSTALL-DIR`, the
driver dependencies will also be copied (e.g. C:\myproject\dependencies\libs)

#### Building the Driver with the Tests

##### Integration Tests

To build integration tests with static linking:

```dos
VC_BUILD.BAT --STATIC --ENABLE-INTEGRATION-TESTS
```

To build unit tests with static linking:

##### Unit Tests
```dos
VC_BUILD.BAT --STATIC --ENABLE-UNIT-TESTS
```

[Kerberos for Windows]: http://web.mit.edu/kerberos/dist/index.html
