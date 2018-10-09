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
DISTRO=${WORKER_INFORMATION[0]}
RELEASE=${WORKER_INFORMATION[1]}
SHA=$(echo ${GIT_COMMIT} | cut -c1-7)
PROCS=$(grep -e '^processor' -c /proc/cpuinfo)

get_driver_version() {
  local header_file=$1
  local driver_prefix=$2
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

configure_environment() {
  if ! grep -lq "127.254.254.254" /etc/hosts; then
    printf "\n\n%s\n" "127.254.254.254  cpp-driver.hostname." | sudo tee -a /etc/hosts
  fi
  sudo cat /etc/hosts
}

install_libuv() {
  (
    cd packaging
    git clone --depth 1 https://github.com/datastax/libuv-packaging.git

    (
      cd libuv-packaging
      if [ "${DISTRO}" = "ubuntu" ]; then
        ./build_deb.sh ${LIBUV_VERSION}
      else
        ./build_rpm.sh ${LIBUV_VERSION}
      fi
    )

    [[ -d packages ]] || mkdir packages
    find libuv-packaging/build -type f \( -name "*.deb" -o -name "*.rpm" \) -exec mv {} packages \;

    if [ "${DISTRO}" = "ubuntu" ]; then
      sudo dpkg -i packages/libuv*.deb
    else
      sudo rpm -i packages/libuv*.rpm
    fi
  )
}

install_dependencies() {
  install_libuv
}

build_driver() {
  local driver_prefix=$1

  (
    [[ -d build ]] || mkdir build
    cd build
    cmake -DCMAKE_BUILD_TYPE=Release -D${driver_prefix}_BUILD_SHARED=On -D${driver_prefix}_BUILD_STATIC=On -D${driver_prefix}_BUILD_EXAMPLES=On -D${driver_prefix}_BUILD_UNIT_TESTS=On ..
    make -j${PROCS}
  )
}

install_driver() {
  (
    cd packaging

    (
      if [ "${DISTRO}" = "ubuntu" ]; then
        ./build_deb.sh
      else
        ./build_rpm.sh
      fi
    )

    [[ -d packages ]] || mkdir packages
    find build -type f \( -name "*.deb" -o -name "*.rpm" \) -exec mv {} packages \;

    if [ "${DISTRO}" = "ubuntu" ]; then
      sudo dpkg -i packages/*cpp-driver*.deb
    else
      sudo rpm -i packages/*cpp-driver*.rpm
    fi
  )
}

test_installed_driver() {
  local driver=$1

  local test_program=$(mktemp)
  gcc -x c -o ${test_program} - -Wno-implicit-function-declaration -l${driver} - <<EOF
#include <${driver}.h>

int main(int argc, char* argv[]) {
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();

  cass_cluster_set_contact_points(cluster, "127.0.0.1");
  connect_future = cass_session_connect(session, cluster);
  cass_future_wait(connect_future);
  printf("Success");
  return 0;
}
EOF

  if [ $? -ne 0 ] ; then
    echo "Connection test compilation failed. Marking build as failure."
    exit 1
  fi
  if [ "$($test_program)" != "Success" ] ; then
    echo "Connection test did not return success. Marking build as failure."
    exit 1
  fi
}

check_driver_exports() {(
  set +e  #Disable fail fast for this subshell
  local driver_library=$1
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
