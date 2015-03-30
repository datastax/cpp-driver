# Building

## Supported Platforms

The driver is known to work on CentOS/RHEL 5/6/7, Mac OS X 10.8/10.9 (Mavericks and Yosemite), Ubuntu 12.04/14.04 LTS, and Windows 7 SP1.

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2012/2013.

## Dependencies

### Driver

- [CMake](http://www.cmake.org)
- [libuv (1.x or 0.10.x)](https://github.com/libuv/libuv)
- [OpenSSL](http://www.openssl.org/) (optional)

**NOTE:** Utilizing the default package manager configuration to install dependencies on \*nix based operating systems may result in older versions of dependencies being installed.

### Test Dependencies

- [boost 1.55+](http://www.boost.org)
- [libssh2](http://www.libssh2.org)

## Linux/OS X
The driver has been built using both Clang (Ubuntu 12.04/14.04 and OS X) and GCC (Linux).

### Obtaining Dependencies

#### CentOS/RHEL

##### Additional Requirements for CentOS/RHEL 5
CentOS/RHEL 5 does not contain `git` in its repositories; however RepoForge (formerly RPMforge) has a RPM for this dependency. It can be found [here](http://pkgs.repoforge.org/git/).

###### Download the Appropriate RepoForge Release Package
- [32-bit](http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el5.rf.i386.rpm)
- [64-bit](http://pkgs.repoforge.org/rpmforge-release/rpmforge-release-0.5.3-1.el5.rf.x86_64.rpm)

###### Install Key and RPM Package

```bash
sudo rpm --import http://apt.sw.be/RPM-GPG-KEY.dag.txt
sudo rpm -i rpmforge-release-0.5.3-1.el5.rf.*.rpm
```

##### Dependencies and libuv Installation

```bash
sudo yum install automake cmake gcc-c++ git libtool openssl-devel wget
pushd /tmp
wget http://libuv.org/dist/v1.4.2/libuv-v1.4.2.tar.gz
tar xzf libuv-v1.4.2.tar.gz
pushd libuv-v1.4.2
sh autogen.sh
./configure
sudo make install
popd
popd
```

#### OS X
The driver has been built and tested using the Clang compiler provided by XCode 5.1+. The dependencies were obtained using [Homebrew](http://brew.sh).

```bash
brew install libuv cmake
```

**NOTE:** The driver utilizes the OpenSSL library included with XCode.

#### Ubuntu

##### Additional Requirements for Ubuntu 12.04
Ubuntu 12.04 does not contain libuv in its repositories; however the LinuxJedi PPA has a backport from Ubuntu 14.04 which can be found [here](https://launchpad.net/~linuxjedi/+archive/ubuntu/ppa).

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
CentOS/RHEL does not contain Boost v1.55+ libraries in its repositories; however these can be easily installed from source. Ensure previous version of Boost has been removed by executing the command `sudo yum remove boost*` before proceeding.

```bash
sudo yum install libssh2-devel
pushd /tmp
wget http://sourceforge.net/projects/boost/files/boost/1.57.0/boost_1_57_0.tar.gz/download -O boost_1_57_0.tar.gz
tar xzf boost_1_57_0.tar.gz
pushd boost_1_57_0
./bootstrap.sh --with-libraries=atomic,chrono,date_time,log,program_options,random,regex,system,thread,test
sudo ./b2 cxxflags="-fPIC" install
popd
popd
```

**NOTE:** CentOS/RHEL 5 has known issues when compiling tests with GCC 4.1.2 as it is not a supported Boost compiler.

##### OS X

```bash
brew install boost libssh2
```

##### Ubuntu

###### Additional Requirements for Ubuntu 12.04
Ubuntu 12.04 does not contain Boost v1.55+ C++ libraries in its repositories; however it can be obtained from the Boost PPA which can be found [here](https://launchpad.net/~boost-latest/+archive/ubuntu/ppa).

```bash
sudo add-apt-repository ppa:boost-latest/ppa
sudo apt-get update
```

##### Install Dependencies

```bash
sudo apt-get install libboost1.55-all-dev libssh2-1-dev
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
The driver has been built and tested using Microsoft Visual Studio 2010, 2012 and 2013 (using the "Express" and Professional versions) and Windows SDK v7.1, 8.0, and 8.1 on Windows 7 SP1. The library dependencies will automatically download and build; however the following build dependencies will need to be installed.

### Obtaining Build Dependencies
- Download and install [CMake](http://www.cmake.org/download).
 - Make sure to select the option "Add CMake to the system PATH for all users" or "Add CMake to the system PATH for current user".
- Download and install [Git](http://git-scm.com/download/win)
 - Make sure to select the option "Use Git from Windows Command Prompt" or manually add the git executable to the system PATH.
- Download and install [ActiveState Perl](https://www.perl.org/get.html#win32)
 - Make sure to select the option "Add Perl to PATH environment variable".
 - **NOTE:** This build dependency is required if building with OpenSSL support
- Download and install [Python v2.7.x](https://www.python.org/downloads)
 - Make sure to select/install the feature "Add python.exe to Path"

### Building the Driver
A batch script has been created to detect installed versions of Visual Studio (and/or Windows SDK installations) to simplify the build process on Windows. If you have more than one version of Visual Studio (and/or Windows SDK) installed you will be prompted to select which version to use when compiling the driver.

First you will need to open a "Command Prompt" (or Windows SDK Command Prompt) to execute the batch script.

```dos
Usage: VC_BUILD.BAT [OPTION...]

        --DEBUG                         Enable debug build
        --RELEASE                       Enable release build (default)
        --DISABLE-CLEAN                 Disable clean build
        --DISABLE-OPENSSL               Disable OpenSSL support
        --ENABLE-PACKAGES [version]     Enable package generation (*)
        --ENABLE-TESTS [boost-root-dir] Enable test builds
        --ENABLE-ZLIB                   Enable zlib
        --GENERATE-SOLUTION             Generate Visual Studio solution (**)
        --SHARED                        Build shared library (default)
        --STATIC                        Build static library
        --X86                           Target 32-bit build (***)
        --X64                           Target 64-bit build (***)

        --HELP                          Display this message

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

To generate Visual Studio solution file:

```dos
VC_BUILD.BAT --GENERATE-SOLUTION
```

### Test Dependencies and Building the Tests (_NOT REQUIRED_)

#### Obtaining Test Dependencies
Boost v1.55+ is the only external dependency that will need to be obtained in order to build the unit and integration tests.

To simplify the process; pre-built binaries can be obtained [here](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/). Ensure the proper Visual Studio (or Windows SDK) version and architecture is obtained and select from the following list:

- Visual Studio 2010 (Windows SDK 7.1)
 - Boost v1.55 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-10.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-10.0-64.exe/download)
 - Boost v1.56 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-10.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-10.0-64.exe/download)
 - Boost v1.57 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-10.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-10.0-64.exe/download)
- Visual Studio 2012 (Windows SDK 8.0)
 - Boost v1.55 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-11.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-11.0-64.exe/download)
 - Boost v1.56 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-11.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-11.0-64.exe/download)
 - Boost v1.57 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-11.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-11.0-64.exe/download)
- Visual Studio 2013 (Windows SDK 8.1)
 - Boost v1.55 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-12.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.55.0/boost_1_55_0-msvc-12.0-64.exe/download)
 - Boost v1.56 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-12.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.56.0/boost_1_56_0-msvc-12.0-64.exe/download)
 - Boost v1.57 [32-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-12.0-32.exe/download)/[64-bit](http://sourceforge.net/projects/boost/files/boost-binaries/1.57.0/boost_1_57_0-msvc-12.0-64.exe/download)

**NOTE:** Ensure the Boost library directory structure is configured correctly by renaming the library directory to _lib_ (e.g. lib64-msvc-12.0 to lib).

#### Building the Driver with the Tests

```dos
VC_BUILD.BAT --STATIC --ENABLE-TESTS <ABSOLUTE-PATH-TO-BOOST>

[e.g. C:\local\boost_1_57_0]
```
