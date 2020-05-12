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
  true
}

install_libuv() {
  if brew ls --versions libuv > /dev/null; then
    if ! brew outdated libuv; then
      brew upgrade libuv
    fi
  else
    brew install libuv
  fi
}

install_openssl() {
  if brew ls --versions openssl > /dev/null; then
    if ! brew outdated openssl; then
      brew upgrade openssl
    fi
  else
    brew install openssl
  fi
}

install_zlib() {
  if brew ls --versions zlib > /dev/null; then
    if ! brew outdated zlib; then
      brew upgrade zlib
    fi
  else
    brew install zlib
  fi
}

install_driver() {
  true
}

test_installed_driver() {
  true
}

