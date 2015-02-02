# Building

## Supported Platforms

The driver is known to work on OS X 10.9, Windows 7, CentOS/RHEL 5/6, and Ubuntu 12.04/14.04. 

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2012/2013.

## Dependencies

- [CMake](http://www.cmake.org)
- [libuv (1.x or 0.10.x)](https://github.com/libuv/libuv)
- [OpenSSL](http://www.openssl.org/) (optional)

**NOTE:** Utilizing the default package manager configuration to install dependencies on \*nix based operating systems may result in older versions of dependecies being retrieved. OpenSSL is of the upmost concern as each new version addresses security [vulnerabilities](https://www.openssl.org/news/vulnerabilities.html) that have been identifed and corrected.

### Test Dependencies

- [boost 1.55+](http://www.boost.org)
- [libssh2](http://www.libssh2.org)
- [ccm](https://github.com/pcmanus/ccm)

## OS X
The driver has been built and tested using the Clang compiler provided by XCode 5.1. The dependencies were obtained using [Homebrew](http://brew.sh).

### Obtaining Dependencies

```bash
brew install libuv cmake
```

**NOTE:** The driver utilizes the OpenSSL library included with XCode.

### Building

```bash
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```
### Test Dependencies and Building Tests (NOT REQUIRED)

#### Obtaining Test Dependencies

```bash
brew install boost libssh2
```

#### Building with the Testing Framework

```bash
cmake -DCASS_BUILD_TESTS=ON .
make
```

## Windows
The driver has been built and tested using Microsoft Visual Studio 2010, 2012 and 2013 (using the "Express" and Professional versions) and Windows SDK v7.1, 8.0, and 8.1 on Windows 7 SP1. The library dependencies will automatically download and build; however the following build dependencies will need to be installed.

### Windows Build Dependencies
To obtain build dependencies:

- Download and install [CMake](http://www.cmake.org/download).
 - Make sure to select the option "Add CMake to the system PATH for all users" or "Add CMake to the system PATH for current user".
- Download and install [Git](http://git-scm.com/download/win)
 - Make sure to select the option "Use Git from Windows Command Prompt" or manually add the git executable to the system PATH.
 - NOTE: This build dependency is required if building with OpenSSL support
- Download and install [ActiveState Perl](https://www.perl.org/get.html#win32)
 - Make sure to select the option "Add Perl to PATH environment variable".
- Download and install [Python v2.7.x](https://www.python.org/downloads)
 - Make sure to select/install the feature "Add python.exe to Path"

### Performing the Windows Build
A batch script has been created to detect installed Visual Studio version(s) (and/or Windows SDK installtion) in order to simplify the build operations on Windows.  If you have more than one version of Visual Studio (and/or Windows SDK) installed you will be prompted to select which version to use when compiling the driver.

First you will need to open a "Command Prompt" (or Windows SDK Command Prompt) to execute the batch script.

```dos
Usage: VC_BUILD.BAT [OPTION...]

        --DEBUG                         Enable debug build
        --RELEASE                       Enable release build (default)
        --DISABLE-CLEAN                 Disable clean build
        --DISABLE-OPENSSL               Disable OpenSSL support
        --ENABLE-PACKAGES [version]     Enable package generation (**)
        --SHARED                        Build shared library (default)
        --STATIC                        Build static library
        --X86                           Target 32-bit build (*)
        --X64                           Target 64-bit build (*)

        --HELP                          Display this message

*  Default target architecture is determined based on system architecture
** Packages are only generated using detected installations of Visual Studio
```

To build 32-bit shared library:

```dos
VC_BUILD.BAT --X86
```

To build 64-bit shared library:

```dos
VC_BUILD.BAT --X64
```

To build static library:

```dos
VC_BUILD.BAT --STATIC
```

To build library without OpenSSL support:

```dos
VC_BUILD.BAT --DISABLE-OPENSSL
```

To build 32-bit static library without OpenSSL support:

```dos
VC_BUILD.BAT --DISABLE-OPENSSL --STATIC --X86
```

## Linux
The driver has been built using both Clang and GCC while being tested with GCC compiled source on Ubuntu 12.04/14.04.

### Obtaining Dependencies

#### Ubuntu

##### Additional Requirements for Ubuntu 12.04 LTS
Ubuntu 12.04 does not have libuv in its repositories; however LinuxJedi created a PPA for this dependency which is a backport from Ubuntu 14.04 LTS. It can be found at: https://launchpad.net/~linuxjedi/+archive/ubuntu/ppa.

```bash
sudo apt-add-repository ppa:linuxjedi/ppa
sudo apt-get update
```

#### GCC

```bash
sudo apt-get install g++ make cmake libuv-dev libssl-dev
```

#### Clang

```bash
sudo apt-get install clang make cmake libuv-dev libssl-dev
```

To build:

```bash
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

### Test Dependencies and Building Tests (NOT REQUIRED)

#### Obtaining Test Dependencies

##### Ubuntu 12.04 LTS

```bash
sudo add-apt-repository ppa:boost-latest/ppa
sudo apt-get update
sudo apt-get install libboost1.55-all-dev libssh2-1-dev
```

##### Ubuntu 14.04 LTS

```bash
sudo apt-get install libboost-chrono-dev libboost-date-time-dev libboost-log-dev libboost-system-dev libboost-thread-dev libboost-test-dev libssh2-1-dev
```

#### Building with the Testing Framework

```bash
cmake -DCASS_BUILD_TESTS=ON .
make
```
