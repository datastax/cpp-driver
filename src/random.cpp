/*
  Copyright (c) 2014-2015 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#if defined(_WIN32)
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#include <Windows.h>
#include <WinCrypt.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#endif

#include "logger.hpp"

namespace cass {

#if defined(_WIN32)

  uint64_t get_random_seed(uint64_t seed) {
    Logger::init();

    HCRYPTPROV provider;

    if (!CryptAcquireContext(&provider, 
                             NULL, NULL, 
                             PROV_RSA_FULL, CRYPT_VERIFYCONTEXT | CRYPT_SILENT)) {
      LOG_CRITICAL("Unable to aqcuire cryptographic provider: 0x%x", GetLastError());
      return seed;
    }

    if (!CryptGenRandom(provider, sizeof(seed), (BYTE*)&seed)) {
      LOG_CRITICAL("An error occurred attempting to generate random data: 0x%x", GetLastError());
      return seed;
    }

    CryptReleaseContext(provider, 0);

    return seed;
  }

#else

  uint64_t get_random_seed(uint64_t seed) {
    Logger::init();

    static const char* device = "/dev/urandom";

    int fd = open(device, O_RDONLY);

    if (fd < 0) {
      char buf[128];
      strerror_r(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to open random device (%s): %s", device, buf);
      return seed;
    }

    ssize_t num_bytes = read(fd, reinterpret_cast<char*>(&seed), sizeof(seed));
    if (num_bytes < 0) {
      char buf[128];
      strerror_r(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read from random device (%s): %s", device, buf);
    } else if (num_bytes != sizeof(seed)) {
      char buf[128];
      strerror_r(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read full seed value (expected: %u read: %u) "
                   "from random device (%s): %s",
                   static_cast<unsigned int>(sizeof(seed)),
                   static_cast<unsigned int>(num_bytes),
                   device, buf);
    }

    close(fd);

    return seed;
  }

#endif

} // namespace cass
