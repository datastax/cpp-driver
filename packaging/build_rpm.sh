#!/bin/bash

function check_command {
  command=$1
  package=$2
  if ! type "$command" > /dev/null 2>&1; then
    echo "Missing command '$command', run: yum install $package"
    exit 1
  fi
}

version="1.0.0"
base="datastax-cpp-driver-$version"
archive="$base.tar.gz"
files="CMakeLists.txt cmake_uninstall.cmake.in include src README.md LICENSE.txt"

# 'redhat-rpm-config' needs to be installed for the 'debuginfo' package
check_command "rpmbuild" "rpm-build"

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

echo "Archiving $archive"
pushd "build/SOURCES"
tar zcf $archive $base
popd

echo "Building package:"
rpmbuild --define "_topdir ${PWD}/build" -ba cpp-driver.spec

exit 0
