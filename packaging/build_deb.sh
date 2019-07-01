#!/bin/bash

function check_command {
  command=$1
  package=$2
  if ! type "$command" > /dev/null 2>&1; then
    echo "Missing command '$command', run: apt-get install $package"
    exit 1
  fi
}

function header_version {
  read -d ''  version_script << 'EOF'
  BEGIN { major="?"; minor="?"; patch="?" }
  /CASS_VERSION_MAJOR/ { major=$3 }
  /CASS_VERSION_MINOR/ { minor=$3 }
  /CASS_VERSION_PATCH/ { patch=$3 }
  /CASS_VERSION_SUFFIX/ { suffix=$3; gsub(/"/, "", suffix) }
  END {
    if (length(suffix) > 0)
      printf "%s.%s.%s~%s", major, minor, patch, suffix
    else
      printf "%s.%s.%s", major, minor, patch
  }
EOF
  version=$(grep '#define[ \t]\+CASS_VERSION_\(MAJOR\|MINOR\|PATCH\|SUFFIX\)' $1 | awk "$version_script")
  if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(~[a-zA-Z0-9_\-]+)?$ ]]; then
    echo "Unable to extract version from $1"
    exit 1
  fi
  echo "$version"
}

check_command "dch" "devscripts"
check_command "lsb_release" "lsb-release"

version=$(header_version "../include/cassandra.h")
release=1
dist=$(lsb_release -s -c)
base="cassandra-cpp-driver-$version"
archive="$base.tar.gz"
files="CMakeLists.txt cmake cmake_uninstall.cmake.in driver_config.hpp.in include src"

echo "Building version $version"

libuv_version=$(dpkg -s libuv1 | grep 'Version' | awk '{ print $2 }')

if [[ -e $libuv_version ]]; then
  echo "'libuv' required, but not installed"
  exit 1
fi

echo "Using libuv version $libuv_version"

if [[ -d build ]]; then
  read -p "Build directory exists, remove? [y|n] " -n 1 -r
  echo
  if [[ $REPLY =~ ^[Yy]$ ]]; then
    rm -rf build
  fi
fi
mkdir -p "build/$base"

echo "Copying files"
for file in $files; do
  cp -r  "../$file" "build/$base"
done
cp -r debian "build/$base"

pushd "build/$base"
echo "Updating changlog"
dch -m -v "$version-$release" -D $dist "Version $version"
echo "Building package:"
nprocs=$(grep -e '^processor' -c /proc/cpuinfo)
DEB_BUILD_OPTIONS="parallel=$nprocs" debuild -i -b -uc -us
popd
