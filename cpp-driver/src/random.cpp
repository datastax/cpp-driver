/*
  Copyright (c) DataStax, Inc.

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
#include "driver_config.hpp"
#include "logger.hpp"
#include "scoped_lock.hpp"

#if defined(_WIN32)
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#include <WinCrypt.h>
#include <Windows.h>
#else
#if defined(HAVE_GETRANDOM)
#include <linux/random.h>
#include <syscall.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#endif

namespace datastax { namespace internal {

Random::Random()
    // Use high resolution time if we can't get a real random seed
    : rng_(get_random_seed(uv_hrtime())) {
  uv_mutex_init(&mutex_);
}

Random::~Random() { uv_mutex_destroy(&mutex_); }

uint64_t Random::next(uint64_t max) {
  ScopedMutex l(&mutex_);

  if (max == 0) {
    return 0;
  }

  const uint64_t limit = CASS_UINT64_MAX - CASS_UINT64_MAX % max;
  uint64_t r;
  do {
    r = rng_();
  } while (r >= limit);
  return r % max;
}

#if defined(_WIN32)

uint64_t get_random_seed(uint64_t seed) {
  HCRYPTPROV provider;

  if (!CryptAcquireContext(&provider, NULL, NULL, PROV_RSA_FULL,
                           CRYPT_VERIFYCONTEXT | CRYPT_SILENT)) {
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
#if defined(HAVE_ARC4RANDOM)
  arc4random_buf(&seed, sizeof(seed));
#else
  static const char* device = "/dev/urandom";
  ssize_t num_bytes;
  bool readurandom = true;
#if defined(HAVE_GETRANDOM)
  num_bytes = static_cast<ssize_t>(syscall(SYS_getrandom, &seed, sizeof(seed), GRND_NONBLOCK));
  if (num_bytes < static_cast<ssize_t>(sizeof(seed))) {
    char buf[STRERROR_BUFSIZE_];
    char* err = STRERROR_R_(errno, buf, sizeof(buf));
    LOG_WARN("Unable to read %u random bytes (%s): %u read",
             static_cast<unsigned int>(sizeof(seed)), err, static_cast<unsigned int>(num_bytes));
  } else {
    readurandom = false;
  }
#endif // defined(HAVE_GETRANDOM)

  if (readurandom) {
    int fd = open(device, O_RDONLY);

    if (fd < 0) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to open random device (%s): %s", device, err);
      return seed;
    }

    num_bytes = read(fd, reinterpret_cast<char*>(&seed), sizeof(seed));
    if (num_bytes < 0) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read from random device (%s): %s", device, err);
    } else if (num_bytes != sizeof(seed)) {
      char buf[STRERROR_BUFSIZE_];
      char* err = STRERROR_R_(errno, buf, sizeof(buf));
      LOG_CRITICAL("Unable to read full seed value (expected: %u read: %u) "
                   "from random device (%s): %s",
                   static_cast<unsigned int>(sizeof(seed)), static_cast<unsigned int>(num_bytes),
                   device, err);
    }

    close(fd);
  }
#endif // defined(HAVE_ARC4RANDOM)

  return seed;
}
#endif

}} // namespace datastax::internal
