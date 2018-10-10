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

#include "loop_test.hpp"

#include "callback.hpp"
#include "name_resolver.hpp"

#ifdef WIN32
#  include "winsock.h"
#endif

#define RESOLVE_TIMEOUT 2000

using cass::Address;
using cass::bind_callback;
using cass::Memory;
using cass::NameResolver;
using cass::String;

class NameResolverUnitTest : public LoopTest {
public:
  NameResolverUnitTest()
    : status_(NameResolver::NEW) { }

  NameResolver::Ptr create(const Address& address) {
    return NameResolver::Ptr(
          Memory::allocate<NameResolver>(address,
                                         bind_callback(&NameResolverUnitTest::on_resolve, this)));
  }

  NameResolver::Status status() const {
    return status_;
  }

  const String& hostname() const {
    return hostname_;
  }

private:
  void on_resolve(NameResolver* resolver) {
    status_ = resolver->status();
    hostname_ = resolver->hostname();
  }

private:
  NameResolver::Status status_;
  String hostname_;
};

TEST_F(NameResolverUnitTest, Simple) {
  NameResolver::Ptr resolver(create(Address("127.0.0.1", 9042)));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(NameResolver::SUCCESS, status());
#ifdef WIN32
  char win_hostname[64];
  gethostname(win_hostname, 64);
  EXPECT_EQ(String(win_hostname), hostname());
#else
  EXPECT_EQ("localhost", hostname());
#endif
}

TEST_F(NameResolverUnitTest, Timeout) {
  NameResolver::Ptr resolver(create(Address("127.0.0.1", 9042)));

  // Libuv's address resolver uses the uv_work thread pool to handle resolution
  // asynchronously. If we starve all the threads in the uv_work thread pool
  // then it will prevent the resolver work from completing before the timeout.
  // This work must be queued before the resolver's work.
  starve_thread_pool(200);

  resolver->resolve(loop(), 1); // Use shortest possible timeout
  run_loop();
  ASSERT_EQ(NameResolver::FAILED_TIMED_OUT, status());
  EXPECT_TRUE(hostname().empty());
}

TEST_F(NameResolverUnitTest, Invalid) {
  NameResolver::Ptr resolver(create(Address()));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(NameResolver::FAILED_BAD_PARAM, status());
  EXPECT_TRUE(hostname().empty());
}

TEST_F(NameResolverUnitTest, Cancel) {
  NameResolver::Ptr resolver(create(Address("127.0.0.1", 9042)));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  resolver->cancel();
  run_loop();
  EXPECT_EQ(NameResolver::CANCELED, status());
  EXPECT_TRUE(hostname().empty());
}
