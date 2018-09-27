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

WORKING_DIRECTORY="$(pwd)"
BASH_FILENAME="$(basename $0)"
BOOST_FILE_VERSION=${BOOST_VERSION//./_}

get_procs() {
  local procs=1
  if [ "${TRAVIS_OS_NAME}" = "linux" ]
  then
    procs=$(nproc)
  elif [ "${TRAVIS_OS_NAME}" = "osx" ];
  then
    procs=$(sysctl -n hw.ncpu)
  fi

  echo ${procs}
}

execute_command() {
  echo "${@}"
  "${@}"

  local error_code=$?
  if [ ${error_code} -ne 0 ]
  then
    if [ "${TRAVIS}" == "true" ]
    then
      travis_terminate ${error_code}
    else
      exit ${error_code}
    fi
  fi
}

configure_environment() {
  if ! grep -lq "127.254.254.254" /etc/hosts
  then
    printf "\n\n%s\n" "127.254.254.254  cpp-driver.hostname." | sudo tee -a /etc/hosts
  fi
  sudo cat /etc/hosts

  if [ "${TRAVIS_OS_NAME}" = "osx" ];
  then
      echo "Opening for 127.0.0.x"
      for i in {0..255}
      do
        sudo ifconfig lo0 alias 127.0.0.$i up
      done
      echo "Opening for 127.254.254.254"
      sudo ifconfig lo0 alias 127.254.254.254 up
  fi
}

install_boost() {
  if [ ! -d "${HOME}/dependencies/boost_${BOOST_FILE_VERSION}" ]
  then
    execute_command wget -q --no-check-certificate http://sourceforge.net/projects/boost/files/boost/${BOOST_VERSION}/boost_${BOOST_FILE_VERSION}.tar.gz/download -O boost_${BOOST_FILE_VERSION}.tar.gz
    execute_command tar xzf boost_${BOOST_FILE_VERSION}.tar.gz
    (
      cd boost_${BOOST_FILE_VERSION}
      execute_command ./bootstrap.sh --with-libraries=chrono,system,thread,test --prefix=${HOME}/dependencies/boost_${BOOST_FILE_VERSION}
      execute_command ./b2 -d0 -j$(get_procs) install
    )
  fi
}

install_libuv() {
  if [ ! -d "${HOME}/dependencies/libuv-${LIBUV_VERSION}" ]
  then
    execute_command wget -q --no-check-certificate http://dist.libuv.org/dist/v${LIBUV_VERSION}/libuv-v${LIBUV_VERSION}.tar.gz
    execute_command tar xzf libuv-v${LIBUV_VERSION}.tar.gz
    (
      cd libuv-v${LIBUV_VERSION}
      [[ -d build ]] || mkdir build
      cd build
      execute_command cmake -DCMAKE_INSTALL_PREFIX=${HOME}/dependencies/libuv-${LIBUV_VERSION} -DBUILD_SHARED_LIBS=On -DBUILD_TESTING=Off -DCMAKE_BUILD_TYPE=Release ..
      execute_command make -j$(get_procs) install
    )
  fi
}

install_dependencies() {
  if [ "${TRAVIS_OS_NAME}" = "linux" ]
  then
    sudo apt-get install libssh2-1-dev libssl-dev
  elif [ "${TRAVIS_OS_NAME}" = "osx" ];
  then
    brew update
    brew install libssh2 openssl
  fi

  install_boost
  install_libuv
}

build_driver() {
  (
    [[ -d build ]] || mkdir build
    cd build
    execute_command cmake -DCMAKE_BUILD_TYPE=Release -DBOOST_ROOT_DIR=${HOME}/dependencies/boost_${BOOST_FILE_VERSION} -DLIBUV_ROOT_DIR=${HOME}/dependencies/libuv-${LIBUV_VERSION}/ -DCASS_BUILD_SHARED=On -DCASS_BUILD_STATIC=On -DCASS_BUILD_EXAMPLES=On -DCASS_BUILD_TESTS=On -DCASS_USE_LIBSSH2=On ..
    execute_command make -j$(get_procs)
  )
}

execute_driver_unit_tests() {
  execute_command build/cassandra-unit-tests
}

check_driver_exports() {
  if [ -f build/libcassandra_static.a ]
  then
    declare -a FUNCTIONS MISSING_FUNCTIONS
    FUNCTIONS=($(grep -Eoh '^cass_\s*(\w+)\s*\(' include/cassandra.h | awk -F '(' '{print $1}'))
    for function in "${FUNCTIONS[@]}"
    do
      nm build/libcassandra_static.a | grep ${function} > /dev/null
      if [ $? -ne 0 ]
      then
        MISSING_DEFINITION+=("${function}")
      fi
    done
    if [ ! -z "${MISSING_DEFINITION}" ]
    then
      printf "Function(s) have no definition:\n"
      for function in ${MISSING_DEFINITION[@]}
      do
        printf "  ${function}\n"
      done
      if [ "${TRAVIS}" == "true" ]
      then
        travis_terminate 1
      else
        exit 1
      fi
    fi
  fi
}
