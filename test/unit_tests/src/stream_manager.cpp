#define BOOST_TEST_DYN_LINK
#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "stream_manager.hpp"

#include <boost/test/unit_test.hpp>


BOOST_AUTO_TEST_SUITE(streams)

BOOST_AUTO_TEST_CASE(test_simple)
{
  cass::StreamManager<int> streams;

  for (int i = 0; i < 128; ++i) {
    int8_t stream = streams.acquire_stream(i);
    BOOST_REQUIRE(stream == i);
  }

  // No more streams left
  BOOST_CHECK(streams.acquire_stream(128) < 0);

  for (int i = 0; i < 128; ++i) {
    int item;
    BOOST_CHECK(streams.get_item(i, item));
    BOOST_CHECK(item == i);
  }

  // 127 was the stream last given back
  int stream = streams.acquire_stream(0);
  BOOST_CHECK(stream == 127);
}

BOOST_AUTO_TEST_CASE(test_alloc)
{
  cass::StreamManager<int> streams;

  for (int i = 0; i < 5; ++i) {
    int8_t stream = streams.acquire_stream(i);
    BOOST_REQUIRE(stream == i);
  }

  for (int i = 0; i < 5; ++i) {
    int item;
    BOOST_CHECK(streams.get_item(i, item, false));
    BOOST_CHECK(item == i);
  }

  // Release streams in "random" order
  streams.release_stream(3);
  streams.release_stream(0);
  streams.release_stream(2);
  streams.release_stream(4);
  streams.release_stream(1);

  // Verify that streams are reused
  BOOST_CHECK(streams.acquire_stream(0) == 1);
  BOOST_CHECK(streams.acquire_stream(0) == 4);
  BOOST_CHECK(streams.acquire_stream(0) == 2);
  BOOST_CHECK(streams.acquire_stream(0) == 0);
  BOOST_CHECK(streams.acquire_stream(0) == 3);

  // Now we should get the first never alloc'd stream
  BOOST_CHECK(streams.acquire_stream(0) == 5);
}

BOOST_AUTO_TEST_SUITE_END()
