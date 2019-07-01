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
#include "resolver.hpp"

#define RESOLVE_TIMEOUT 2000

using namespace datastax;
using namespace datastax::internal;
using namespace datastax::internal::core;

class ResolverUnitTest : public LoopTest {
public:
  ResolverUnitTest()
      : status_(Resolver::NEW) {}

  Resolver::Ptr create(const String& hostname, int port = 9042) {
    return Resolver::Ptr(
        new Resolver(hostname, port, bind_callback(&ResolverUnitTest::on_resolve, this)));
  }

  MultiResolver::Ptr create_multi() {
    return MultiResolver::Ptr(
        new MultiResolver(bind_callback(&ResolverUnitTest::on_multi_resolve, this)));
  }

  Resolver::Status status() const { return status_; }

  const AddressVec& addresses() const { return addresses_; }

  const Resolver::Vec& resolvers() const { return resolvers_; }

  void verify_addresses(const AddressVec& addresses) const {
    ASSERT_FALSE(addresses.empty());
    EXPECT_TRUE(Address("127.0.0.1", 9042) == addresses[0] || Address("::1", 9042) == addresses[0])
        << "Unable to find \"127.0.0.1\" (IPv4) or \"::1\" (IPv6) in " << addresses;
  }

private:
  void on_resolve(Resolver* resolver) {
    status_ = resolver->status();
    addresses_ = resolver->addresses();
  }

  void on_multi_resolve(MultiResolver* resolver) { resolvers_ = resolver->resolvers(); }

private:
  Resolver::Status status_;
  AddressVec addresses_;
  Resolver::Vec resolvers_;
};

TEST_F(ResolverUnitTest, Simple) {
  Resolver::Ptr resolver(create("localhost"));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(Resolver::SUCCESS, status());
  verify_addresses(addresses());
}

TEST_F(ResolverUnitTest, Timeout) {
  Resolver::Ptr resolver(create("localhost"));

  // Libuv's name resolver uses the uv_work thread pool to handle resolution
  // asynchronously. If we starve all the threads in the uv_work thread pool
  // then it will prevent the resolver work from completing before the timeout.
  // This work must be queued before the resolver's work.
  starve_thread_pool(200);

  resolver->resolve(loop(), 1); // Use shortest possible timeout
  run_loop();
  ASSERT_EQ(Resolver::FAILED_TIMED_OUT, status());
  EXPECT_TRUE(addresses().empty());
}

TEST_F(ResolverUnitTest, Invalid) {
  Resolver::Ptr resolver(create("doesnotexist.dne"));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(Resolver::FAILED_UNABLE_TO_RESOLVE, status());
  EXPECT_TRUE(addresses().empty());
}

TEST_F(ResolverUnitTest, Cancel) {
  Resolver::Ptr resolver(create("localhost"));
  resolver->resolve(loop(), RESOLVE_TIMEOUT);
  resolver->cancel();
  run_loop();
  EXPECT_EQ(Resolver::CANCELED, status());
  EXPECT_TRUE(addresses().empty());
}

TEST_F(ResolverUnitTest, Multi) {
  MultiResolver::Ptr resolver(create_multi());
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(3u, resolvers().size());
  for (Resolver::Vec::const_iterator it = resolvers().begin(), end = resolvers().end(); end != it;
       ++it) {
    EXPECT_EQ(Resolver::SUCCESS, (*it)->status());
    verify_addresses((*it)->addresses());
  }
}

TEST_F(ResolverUnitTest, MultiTimeout) {
  MultiResolver::Ptr resolver(create_multi());

  // Libuv's name resolver uses the uv_work thread pool to handle resolution
  // asynchronously. If we starve all the threads in the uv_work thread pool
  // then it will prevent the resolver work from completing before the timeout.
  // This work must be queued before the resolver's work.
  starve_thread_pool(200);

  // Use shortest possible timeout for all requests
  resolver->resolve(loop(), "localhost", 9042, 1);
  resolver->resolve(loop(), "localhost", 9042, 1);
  resolver->resolve(loop(), "localhost", 9042, 1);

  run_loop();
  ASSERT_EQ(3u, resolvers().size());
  for (Resolver::Vec::const_iterator it = resolvers().begin(), end = resolvers().end(); end != it;
       ++it) {
    EXPECT_EQ(Resolver::FAILED_TIMED_OUT, (*it)->status());
    EXPECT_TRUE((*it)->addresses().empty());
  }
}

TEST_F(ResolverUnitTest, MultiInvalid) {
  MultiResolver::Ptr resolver(create_multi());
  resolver->resolve(loop(), "doesnotexist1.dne", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "doesnotexist2.dne", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "doesnotexist3.dne", 9042, RESOLVE_TIMEOUT);
  run_loop();
  ASSERT_EQ(3u, resolvers().size());
  for (Resolver::Vec::const_iterator it = resolvers().begin(), end = resolvers().end(); end != it;
       ++it) {
    EXPECT_EQ(Resolver::FAILED_UNABLE_TO_RESOLVE, (*it)->status());
    EXPECT_TRUE((*it)->addresses().empty());
  }
}

TEST_F(ResolverUnitTest, MultiCancel) {
  MultiResolver::Ptr resolver(create_multi());
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  resolver->resolve(loop(), "localhost", 9042, RESOLVE_TIMEOUT);
  resolver->cancel();
  run_loop();
  ASSERT_EQ(3u, resolvers().size());
  for (Resolver::Vec::const_iterator it = resolvers().begin(), end = resolvers().end(); end != it;
       ++it) {
    EXPECT_EQ(Resolver::CANCELED, (*it)->status());
    EXPECT_TRUE((*it)->addresses().empty());
  }
}
