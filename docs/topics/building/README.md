# Building

## Driver Dependencies

- [CMake](http://www.cmake.org)
- [libuv (1.0x or 0.10)](https://github.com/libuv/libuv)
- [OpenSSL](http://www.openssl.org/) (optional)

## Test Dependencies

- [boost 1.55+](http://www.boost.org)
- [libssh2](http://www.libssh2.org)
- [ccm](https://github.com/pcmanus/ccm)

## Supported Platforms

The driver is known to work on OS X 10.9, Windows 7, RHEL 5/6, and Ubuntu 12.04/14.04. 

It has been built using GCC 4.1.2+, Clang 3.4+, and MSVC 2010/2013.

## OS X
The driver has been built and tested using the Clang compiler provided by XCode 5.1. The dependencies were obtained using [Homebrew](http://brew.sh).

To obtain dependencies:

```bash
brew install libuv cmake
```

Note: We currently use the OpenSSL library included with XCode.

To obtain test dependencies (This is not required):

```bash
brew install boost libssh2
```

To build:

```bash
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

## Windows
The driver has been built and tested using Microsoft Visual Studio 2010 and 2013 (using the "Express" versions) on Windows 7 SP1. The dependencies need to be manually built or obtained.

To obtain dependencies:
* Download and install CMake for Windows. Make sure to select the option "Add CMake to the system PATH for all users" or "Add CMake to the system PATH for current user".
* Download and build the latest release of libuv 0.10 from https://github.com/joyent/libuv/releases.
  1. Follow the instructions [here](https://github.com/joyent/libuv#windows).
  2. Open up the generated Visual Studio solution "uv.sln".
  3. If you want a 64-bit build you will need to create a "x64" solution platform in the "Configuration Manager".
  4. Open "Properties" on the "libuv" project. Set "Multi-threaded DLL (/MD)" for the "Configuration Properties -> C/C++ -> Code Generation -> Runtime Library" option.
  5. Build the "libuv" project
  6. Copy the files in "libuv/include" to "cpp-driver/lib/libuv/include" and "libuv/Release/lib" to "cpp-driver/lib/libuv/lib".
* Download and install either the 32-bit or 64-bit version of OpenSSL from http://slproweb.com/products/Win32OpenSSL.html. You may also need to install the "Visual C++ 2008 Redistributables".

To build 32-bit (using "VS2013 x86 Native Tools Command Prompt"):

```bash
cd <path to driver>/cpp-driver
cmake -G "Visual Studio 12"
msbuild cassandra.vcxproj /p:Configuration=Release /t:Clean,Build
```

To build 64-bit (using "VS2013 x64 Cross Tools Command Prompt"):

```bash
cd <path to driver>/cpp-driver
cmake -G "Visual Studio 12 Win64"
msbuild cassandra.vcxproj /p:Configuration=Release /t:Clean,Build
```

Note: Use "cmake -G "Visual Studio 10" for Visual Studio 2010

## Linux
The driver was built and tested using both GCC and Clang on Ubuntu 14.04.

To obtain dependencies (GCC):

```bash
sudo apt-get install g++ make cmake libuv-dev libssl-dev
```

To obtain dependencies (Clang):

```bash
sudo apt-get install clang make cmake libuv-dev libssl-dev
```

To obtain test dependencies (This is not required):

```bash
sudo apt-get install libboost-chrono-dev libboost-date-time-dev libboost-log-dev libboost-system-dev libboost-thread-dev libboost-test-dev libssh2-1-dev
```

To build:

```bash
git clone https://github.com/datastax/cpp-driver.git
cd cpp-driver
cmake .
make
```

## Building Tests
Tests are not built by default. To build the tests add "-DCASS_BUILD_TESTS=ON" when running cmake.
