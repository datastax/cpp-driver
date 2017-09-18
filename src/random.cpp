/*
  Copyright (c) 2014-2016 DataStax

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

#include "random.hpp"

#include "cassandra.h"
#include "logger.hpp"
#include "scoped_lock.hpp"

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

namespace cass {

Random::Random()
 // Use high resolution time if we can't get a real random seed
  : rng_(get_random_seed(uv_hrtime())) {
}

uint64_t Random::next(uint64_t max) {
  const uint64_t limit = CASS_UINT64_MAX - CASS_UINT64_MAX % max;
  uint64_t r;
  do {
    r = rng_();
  } while(r >= limit);
  return r % max;
}

#if defined(_WIN32)

  uint64_t get_random_seed(uint64_t seed) {
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

#define STRERROR_BUFSIZE_ 256

#if defined(__APPLE__) || defined(__FreeBSD__) || !defined(_GNU_SOURCE) || !defined(__GLIBC__)
#define STRERROR_R_(errno, buf, bufsize) (strerror_r(errno, buf, bufsize), buf)
#else
#define STRERROR_R_(errno, buf, bufsize) strerror_r(errno, buf, bufsize)
#endif

  uint64_t get_random_seed(uint64_t seed) {
    static const char* device = "/dev/urandom";

    int fd = open(device, O_RDONLY);

    if (fd < 0) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to open random device (%s): %s", device, err);
      return seed;
    }

    ssize_t num_bytes = read(fd, reinterpret_cast<char*>(&seed), sizeof(seed));
    if (num_bytes < 0) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read from random device (%s): %s", device, err);
    } else if (num_bytes != sizeof(seed)) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read full seed value (expected: %u read: %u) "
                   "from random device (%s): %s",
                   static_cast<unsigned int>(sizeof(seed)),
                   static_cast<unsigned int>(num_bytes),
                   device, err);
    }

    close(fd);

    return seed;
  }

#endif

} // namespace cass
