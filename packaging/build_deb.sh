#!/bin/bash

function check_command {
  command=$1
  package=$2
  if ! type "$command" > /dev/null 2>&1; then
    echo "Missing command '$command', run: apt-get install $package"
    exit 1
  fi
}

check_command "dch" "debhelper"
check_command "lsb_release" "lsb-release"

version="2.2.1"
release=1
dist=$(lsb_release -s -c)
base="cassandra-cpp-driver-$version"
archive="$base.tar.gz"
files="CMakeLists.txt cmake_uninstall.cmake.in include src"

libuv_version=$(dpkg -s libuv | grep 'Version' | awk '{ print $2 }')

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
