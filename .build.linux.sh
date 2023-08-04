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

configure_testing_environment() {
  if ! grep -lq "127.254.254.254" /etc/hosts; then
    printf "\n\n%s\n" "127.254.254.254  cpp-driver.hostname." | sudo tee -a /etc/hosts
  fi
  sudo cat /etc/hosts
}

install_libuv() {(
  if [[ "${OS_DISTRO}" = "ubuntu" ]] && [[ "${OS_DISTRO_RELEASE}" > "18.04" ]]; then
    true
  else
    cd packaging
    git clone --depth 1 https://github.com/datastax/libuv-packaging.git

    (
      cd libuv-packaging

      # Ensure build directory is cleaned (static nodes are not cleaned)
      [[ -d build ]] && rm -rf build
      mkdir build

      if [ "${OS_DISTRO}" = "ubuntu" ]; then
        ./build_deb.sh ${LIBUV_VERSION}
      else
        ./build_rpm.sh ${LIBUV_VERSION}
      fi
    )

    [[ -d packages ]] || mkdir packages
    find libuv-packaging/build -type f \( -name "*.deb" -o -name "*.rpm" \) -exec mv {} packages \;

    if [ "${OS_DISTRO}" = "ubuntu" ]; then
      sudo dpkg -i packages/libuv*.deb
    else
      sudo rpm -U --force packages/libuv*.rpm
    fi
  fi
)}

install_openssl() {
  true # Already installed on image
}

install_zlib() {
  true # Already installed on image
}

install_driver() {(
  cd packaging

  (
    # Ensure build directory is cleaned (static nodes are not cleaned)
    [[ -d build ]] && rm -rf build
    mkdir build

    if [ "${OS_DISTRO}" = "ubuntu" ]; then
      ./build_deb.sh
    else
      ./build_rpm.sh
    fi
  )

  [[ -d packages ]] || mkdir packages
  find build -type f \( -name "*.deb" -o -name "*.rpm" \) -exec mv {} packages \;

  if [ "${OS_DISTRO}" = "ubuntu" ]; then
    sudo dpkg -i packages/*cpp-driver*.deb
  else
    sudo rpm -i packages/*cpp-driver*.rpm
  fi
)}

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

