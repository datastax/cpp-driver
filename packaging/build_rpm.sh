#!/bin/bash

function check_command {
  command=$1
  package=$2
  if ! type "$command" > /dev/null 2>&1; then
    echo "Missing command '$command', run: yum install $package"
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
      printf "%s.%s.%s%s", major, minor, patch, suffix
    else
      printf "%s.%s.%s", major, minor, patch
  }
EOF
  version=$(grep '#define[ \t]\+CASS_VERSION_\(MAJOR\|MINOR\|PATCH\|SUFFIX\)' $1 | awk "$version_script")
  if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-zA-Z0-9_\-]+)?$ ]]; then
    echo "Unable to extract version from $1"
    exit 1
  fi
  echo "$version"
}

# 'redhat-rpm-config' needs to be installed for the 'debuginfo' package
check_command "rpmbuild" "rpm-build"

arch="x86_64"
if [[ ! -z $1 ]]; then
  arch=$1
fi

version=$(header_version "../include/cassandra.h")
base="cassandra-cpp-driver-$version"
archive="$base.tar.gz"
files="CMakeLists.txt cmake cmake_uninstall.cmake.in driver_config.hpp.in include src README.md LICENSE.txt"

echo "Building version $version"

libuv_version=$(rpm -q --queryformat "%{VERSION}" libuv)

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
mkdir -p build/{BUILD,RPMS,SOURCES,SPECS,SRPMS}
mkdir -p "build/SOURCES/$base"

echo "Copying files"
for file in $files; do
  cp -r  "../$file" "build/SOURCES/$base"
done
cp cassandra.pc.in build/SOURCES
cp cassandra_static.pc.in build/SOURCES

echo "Archiving $archive"
pushd "build/SOURCES"
tar zcf $archive $base
popd

echo "Building package:"
rpmbuild --target $arch --define "_topdir ${PWD}/build" --define "driver_version $version" --define "libuv_version $libuv_version" -ba cassandra-cpp-driver.spec

exit 0
