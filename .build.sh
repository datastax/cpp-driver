#!/bin/bash
##
#  Copyright (c) DataStax, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##

#Debug statements [if needed]
#set -x #Trace
#set -n #Check Syntax
set -e #Fail fast on non-zero exit status

WORKER_INFORMATION=($(echo ${OS_VERSION} | tr "/" " "))
OS_NAME=${WORKER_INFORMATION[0]}
RELEASE=${WORKER_INFORMATION[1]}
SHA=$(echo ${GIT_COMMIT} | cut -c1-7)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
if [ "${OS_NAME}" = "osx" ]; then
  PROCS=$(sysctl -n hw.logicalcpu)
  . ${SCRIPT_DIR}/.build.osx.sh
else
  PROCS=$(grep -e '^processor' -c /proc/cpuinfo)
  . ${SCRIPT_DIR}/.build.linux.sh
fi

get_driver_version() {
  local header_file=${1}
  local driver_prefix=${2}
  local driver_version=$(grep "#define[ \t]\+${driver_prefix}_VERSION_\(MAJOR\|MINOR\|PATCH\|SUFFIX\)" ${header_file}  | awk '
    BEGIN { major="?"; minor="?"; patch="?" }
    /_VERSION_MAJOR/ { major=$3 }
    /_VERSION_MINOR/ { minor=$3 }
    /_VERSION_PATCH/ { patch=$3 }
    /_VERSION_SUFFIX/ { suffix=$3; gsub(/"/, "", suffix) }
    END {
      if (length(suffix) > 0)
        printf "%s.%s.%s-%s", major, minor, patch, suffix
      else
        printf "%s.%s.%s", major, minor, patch
    }
  ')
  if [[ ! ${driver_version} =~ ^[0-9]+\.[0-9]+\.[0-9]+([a-zA-Z0-9_\-]+)?$ ]]
  then
    echo "Unable to extract version from ${header_file} using prefix ${driver_prefix}"
    exit 1
  fi
  echo "${driver_version}"
}

install_dependencies() {
  install_libuv
  install_openssl
  install_zlib
}

build_driver() {
  local driver_prefix=${1}

  # Ensure build directory is cleaned (static nodes are not cleaned)
  [[ -d build ]] && rm -rf build
  mkdir build

  (
    cd build
    BUILD_INTEGRATION_TESTS=Off
    if [ "${CI_INTEGRATION_ENABLED}" == "true" ]; then
      BUILD_INTEGRATION_TESTS=On
    fi
    cmake -DCMAKE_BUILD_TYPE=Release \
          -D${driver_prefix}_BUILD_SHARED=On \
          -D${driver_prefix}_BUILD_STATIC=On \
          -D${driver_prefix}_BUILD_EXAMPLES=On \
          -D${driver_prefix}_BUILD_UNIT_TESTS=On \
          -D${driver_prefix}_BUILD_INTEGRATION_TESTS=${BUILD_INTEGRATION_TESTS} \
          ..
    [[ -x $(which clang-format) ]] && make format-check
    make -j${PROCS}
  )
}

check_driver_exports() {(
  set +e  #Disable fail fast for this subshell
  local driver_library=${1}
  if [ -f ${driver_library} ]; then
    declare -a MISSING_FUNCTIONS
    for function in "${@:2}"; do
      nm ${driver_library} | grep ${function} > /dev/null
      if [ $? -ne 0 ]
      then
        MISSING_DEFINITION+=("${function}")
      fi
    done
    if [ ! -z "${MISSING_DEFINITION}" ]; then
      printf "Function(s) have no definition:\n"
      for function in ${MISSING_DEFINITION[@]}
      do
        printf "  ${function}\n"
      done
      exit 1
    fi
  fi
)}
