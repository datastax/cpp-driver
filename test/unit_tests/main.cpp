// This is free and unencumbered software released into the public domain.

// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.

// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.

// For more information, please refer to <http://unlicense.org/>

#include <stdio.h>
#include <iostream>

#include "../../src/cql_common.hpp"
#include "cql_message.hpp"
#include "cql_ssl_context.hpp"
#include "cql_ssl_session.hpp"
#include "cql_stream_storage.hpp"

char TEST_MESSAGE_ERROR[] = {
  0x81, 0x01, 0x7F, 0x00, 0x00, 0x00, 0x00, 0x0C,  // header
  0xFF, 0xFF, 0xFF, 0xFF,                          // error code
  0x00, 0x06, 0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72   // message
};

char TEST_MESSAGE_OPTIONS[] = {
  0x02, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00   // header
};

char TEST_MESSAGE_STARTUP[] = {
  0x02, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x16,  // header
  0x00, 0x01,                                      // 1 entry
  0x00, 0x0b, 0x43, 0x51, 0x4c, 0x5f, 0x56, 0x45,  // CQL_VERSION
  0x52, 0x53, 0x49, 0x4f, 0x4e,
  0x00, 0x05, 0x33, 0x2e, 0x30, 0x2e, 0x30         // 3.0.0
};

char TEST_MESSAGE_QUERY[] = {
  0x02, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x22,  // header
  0x00, 0x00, 0x00, 0x1b,                          // string length (27)
  0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,              // SELECT
  0x20, 0x2a, 0x20,                                //  *
  0x46, 0x52, 0x4f, 0x4d, 0x20,                    // FROM
  0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x2e,        // system.
  0x70, 0x65, 0x65, 0x72, 0x73, 0x3b,              // peers;
  0x00, 0x01,                                      // consistency
  0x00                                             // flags
};

char TEST_MESSAGE_QUERY_VALUE[] = {
  0x02, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x27,  // header
  0x00, 0x00, 0x00, 0x10,                          // string length (16)
  0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,              // SELECT
  0x20, 0x2a, 0x20,                                //  *
  0x46, 0x52, 0x4f, 0x4d, 0x20,                    // FROM
  0x3f, 0x3b,                                      // ?;
  0x00, 0x01,                                      // consistency
  0x01,                                            // flags
  0x00, 0x01,                                      // values size
  0x00, 0x0c,                                      // value size 12
  0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x2e,        // system.
  0x70, 0x65, 0x65, 0x72, 0x73,                    // peers
};

char TEST_MESSAGE_QUERY_PAGING[] = {
  0x02, 0x00, 0x00, 0x07, 0x00, 0x00, 0x00, 0x2a,  // header
  0x00, 0x00, 0x00, 0x1b,                          // string length (27)
  0x53, 0x45, 0x4c, 0x45, 0x43, 0x54,              // SELECT
  0x20, 0x2a, 0x20,                                //  *
  0x46, 0x52, 0x4f, 0x4d, 0x20,                    // FROM
  0x73, 0x79, 0x73, 0x74, 0x65, 0x6d, 0x2e,        // system.
  0x70, 0x65, 0x65, 0x72, 0x73, 0x3b,              // peers;
  0x00, 0x01,                                      // consistency
  0x08,                                            // flags
  0x00, 0x06,                                      // length 6
  0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72               // foobar
};


#define TEST(x)          if (!x) { return -1; }
#define CHECK(x)         if (!x) { std::cerr << "TEST FAILED AT " << __FILE__ << ":" << __LINE__ << std::endl; return false; }
#define CHECK_EQUAL(x,y) if (x != y) { std::cerr << "TEST FAILED AT " << __FILE__ << ":" << __LINE__  << " " << x << " != " << y << std::endl; return false; }

void
print_hex(
    char*  value,
    size_t size) {
  for (size_t i = 0; i < size; ++i) {
    printf("%02.2x ", (unsigned)(unsigned char) value[i]);
  }
}

bool
test_error_consume() {
  cql::Message message;
  CHECK_EQUAL(
      message.consume(TEST_MESSAGE_ERROR,
                      sizeof(TEST_MESSAGE_ERROR)),
      sizeof(TEST_MESSAGE_ERROR));
  CHECK_EQUAL(static_cast<int>(message.version), 0x81);
  CHECK_EQUAL(static_cast<int>(message.flags), 0x01);
  CHECK_EQUAL(static_cast<int>(message.stream), 0x7F);
  CHECK_EQUAL(static_cast<int>(message.opcode), 0x00);
  CHECK_EQUAL(static_cast<int>(message.length), 0x0C);
  return true;
}

bool
test_error_prepare() {
  cql::Message message;
  message.version = 0x81;
  message.flags   = 0x01;
  message.stream  = 0x7F;
  message.opcode  = 0x00;
  message.body.reset(new cql::BodyError(0xFFFFFFFF, (const char*)"foobar", 6));

  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;
  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);

  CHECK_EQUAL(sizeof(TEST_MESSAGE_ERROR), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_ERROR,
             buffer.get(),
             sizeof(TEST_MESSAGE_ERROR)),
      0);
  return true;
}

bool
test_options_prepare() {
  cql::Message          message(CQL_OPCODE_OPTIONS);
  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;

  CHECK(message.body);
  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);

  CHECK_EQUAL(sizeof(TEST_MESSAGE_OPTIONS), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_OPTIONS,
             buffer.get(),
             sizeof(TEST_MESSAGE_OPTIONS)),
      0);
  return true;
}

bool
test_startup_prepare() {
  cql::Message          message(CQL_OPCODE_STARTUP);
  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;

  CHECK(message.body);
  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);

  CHECK_EQUAL(sizeof(TEST_MESSAGE_STARTUP), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_STARTUP,
             buffer.get(),
             sizeof(TEST_MESSAGE_STARTUP)),
      0);
  return true;
}

bool
test_query_query() {
  cql::Message          message(CQL_OPCODE_QUERY);
  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;

  CHECK(message.body);
  cql::BodyQuery* query = static_cast<cql::BodyQuery*>(message.body.get());
  query->query_string("SELECT * FROM system.peers;");
  query->consistency(CQL_CONSISTENCY_ONE);

  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);
  CHECK_EQUAL(sizeof(TEST_MESSAGE_QUERY), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_QUERY,
             buffer.get(),
             sizeof(TEST_MESSAGE_QUERY)),
      0);
  return true;
}

bool
test_query_query_value() {
  cql::Message          message(CQL_OPCODE_QUERY);
  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;
  const char*           value = "system.peers";

  CHECK(message.body);
  cql::BodyQuery* query = static_cast<cql::BodyQuery*>(message.body.get());
  query->query_string("SELECT * FROM ?;");
  query->add_value(value, strlen(value));
  query->consistency(CQL_CONSISTENCY_ONE);

  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);

  print_hex(buffer_ptr, size);
  std::cout << std::endl;

  print_hex(TEST_MESSAGE_QUERY_VALUE, sizeof(TEST_MESSAGE_QUERY_VALUE));
  std::cout << std::endl;

  CHECK_EQUAL(sizeof(TEST_MESSAGE_QUERY_VALUE), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_QUERY_VALUE,
             buffer.get(),
             sizeof(TEST_MESSAGE_QUERY_VALUE)),
      0);
  return true;
}

bool
test_query_query_paging() {
  cql::Message          message(CQL_OPCODE_QUERY);
  std::unique_ptr<char> buffer;
  char*                 buffer_ptr;
  size_t                size;
  const char*           paging_state = "foobar";

  CHECK(message.body);
  cql::BodyQuery* query = static_cast<cql::BodyQuery*>(message.body.get());
  query->query_string("SELECT * FROM system.peers;");
  query->consistency(CQL_CONSISTENCY_ONE);
  query->paging_state(paging_state, strlen(paging_state));

  CHECK(message.prepare(&buffer_ptr, size));
  buffer.reset(buffer_ptr);

  CHECK_EQUAL(sizeof(TEST_MESSAGE_QUERY_PAGING), size);
  CHECK_EQUAL(
      memcmp(TEST_MESSAGE_QUERY_PAGING,
             buffer.get(),
             sizeof(TEST_MESSAGE_QUERY_PAGING)),
      0);
  return true;
}

bool
test_stream_storage() {
  typedef cql::StreamStorage<int, int, 127> StreamStorageCollection;

  StreamStorageCollection streams;
  {
    int stream = 0;
    CHECK(!streams.set_stream(1, stream));
    CHECK_EQUAL(1, stream);
  }

  {
    int stream = 0;
    CHECK(!streams.get_stream(1, stream));
    CHECK_EQUAL(1, stream);
  }

  {
    int stream = 0;
    CHECK(!streams.set_stream(1, stream));
    CHECK_EQUAL(1, stream);
  }

  {
    int stream = 0;
    CHECK(!streams.get_stream(1, stream));
    CHECK_EQUAL(1, stream);
  }

  for (int i = 1; i <= 127; ++i) {
    int stream = 0;
    CHECK(!streams.set_stream(i, stream));
    CHECK_EQUAL(i, stream);
  }

  {
    // set stream should fail
    int stream = 0;
    CHECK(streams.set_stream(128, stream));
    CHECK_EQUAL(0, stream);
  }

  for (int i = 127; i >= 1; --i) {
    int stream = 0;
    CHECK(!streams.get_stream(i, stream));
    CHECK_EQUAL(i, stream);
  }

  {
    // get stream should fail
    int stream = 0;
    CHECK(streams.get_stream(1, stream));
  }

  {
    // get stream should succeed, because we're not releasing
    int stream = 0;
    CHECK(!streams.get_stream(1, stream, false));
  }

  return true;
}


bool
test_ssl() {
  cql::SSLContext ssl_client_context;
  ssl_client_context.init(true, true);

  cql::SSLContext ssl_server_context;
  ssl_server_context.init(true, false);

  RSA* rsa = cql::SSLContext::create_key(2048);
  if (!rsa) {
    std::cerr << "create_key" << std::endl;
    return 1;
  }

  const char* pszCommonName = "test name";
  X509* cert = cql::SSLContext::create_cert(
      rsa, rsa, pszCommonName, pszCommonName, "DICE", 3 * 365 * 24 * 60 * 60);
  CHECK(cert);
  ssl_server_context.use_key(rsa);
  ssl_server_context.use_cert(cert);

  std::unique_ptr<cql::SSLSession> client_session(
      ssl_client_context.session_new());

  std::unique_ptr<cql::SSLSession> server_session(
      ssl_server_context.session_new());

  CHECK(client_session->init());
  CHECK(server_session->init());
  client_session->handshake(true);
  server_session->handshake(false);

  uv_buf_t client_write_input  = uv_buf_init(NULL, 0);
  uv_buf_t client_write_output = uv_buf_init(NULL, 0);
  uv_buf_t client_read_output  = uv_buf_init(NULL, 0);

  uv_buf_t server_write_input  = uv_buf_init(NULL, 0);
  uv_buf_t server_write_output = uv_buf_init(NULL, 0);
  uv_buf_t server_read_output  = uv_buf_init(NULL, 0);

  bool ssl_established = false;
  bool client_string_received = false;
  bool server_string_received = false;

  const char* client_string = "hello";
  const char* server_string = "ehllo";

  for (;;) {
    size_t client_read = 0;
    cql::Error* err = client_session->read_write(
        server_write_output.base,
        server_write_output.len,
        client_read,
        &(client_read_output.base),
        client_read_output.len,
        client_write_input.base,
        client_write_input.len,
        &(client_write_output.base),
        client_write_output.len);

    CHECK(!err);

    // std::cout << "client_session->read_write: " << client_read_write;
    // std::cout << ", client_read: " << client_read;
    // std::cout << ", client_read_output.len: " << client_read_output.len;
    // std::cout << ", client_write_input.len: " << client_write_input.len;
    // std::cout << ", client_write_output.len: " << client_write_output.len;
    // std::cout << std::endl;

    if (server_write_output.len) {
      server_write_output.len = 0;
      delete server_write_output.base;
    }
    if (client_read_output.len) {
      if (ssl_established) {
        CHECK_EQUAL(0, memcmp(client_read_output.base, server_string, strlen(server_string)));
        CHECK_EQUAL(strlen(server_string), client_read_output.len);
        server_string_received = true;
      }
      client_read_output.len = 0;
      delete client_read_output.base;
    }
    if (client_write_input.len) {
      client_write_input.len = 0;
      delete client_write_input.base;
    }

    size_t server_read = 0;
    err = server_session->read_write(
        client_write_output.base,
        client_write_output.len,
        server_read,
        &(server_read_output.base),
        server_read_output.len,
        server_write_input.base,
        server_write_input.len,
        &(server_write_output.base),
        server_write_output.len);

    CHECK(!err);

    // std::cout << "server_session->read_write: " << server_read_write;
    // std::cout << ", server_read: " << server_read;
    // std::cout << ", server_read_output.len: " << server_read_output.len;
    // std::cout << ", server_write_input.len: " << server_write_input.len;
    // std::cout << ", server_write_output.len: " << server_write_output.len;
    // std::cout << std::endl;

    if (client_write_output.len) {
      client_write_output.len = 0;
      delete client_write_output.base;
    }
    if (server_read_output.len) {
      if (ssl_established) {
        // print_hex(server_read_output.base, server_read_output.len);
        // std::cout << std::endl;
        // printf("%.*s\n", static_cast<int>(server_read_output.len), server_read_output.base);
        CHECK_EQUAL(0, memcmp(server_read_output.base, client_string, strlen(client_string)));
        CHECK_EQUAL(strlen(client_string), server_read_output.len);
        client_string_received = true;
      }
      server_read_output.len = 0;
      delete server_read_output.base;
    }
    if (server_write_input.len) {
      server_write_input.len = 0;
      delete server_write_input.base;
    }

    if (!ssl_established
        && server_session->handshake_done()
        && client_session->handshake_done()) {
      // char ciphers[1024];

      // client_session->ciphers(ciphers, sizeof(ciphers));
      // std::cout << ciphers << std::endl;

      // client_session->ciphers(ciphers, sizeof(ciphers));
      // std::cout << ciphers << std::endl;

      client_write_input.base = new char[strlen(client_string)];
      strcpy(client_write_input.base, client_string);
      client_write_input.len = strlen(client_string);

      server_write_input.base = new char[strlen(server_string)];
      strcpy(server_write_input.base, server_string);
      server_write_input.len = strlen(server_string);

      ssl_established = true;
    }

    if (!client_read
        && !server_read
        && !client_read_output.len
        && !server_read_output.len
        && !client_write_output.len
        && !server_write_output.len
        && client_string_received
        && server_string_received) {
      break;
    }
  }

  CHECK(client_string_received
        && server_string_received);

  return true;
}

int
main() {
  TEST(test_error_consume());
  TEST(test_error_prepare());
  TEST(test_options_prepare());
  TEST(test_startup_prepare());
  TEST(test_query_query());
  TEST(test_query_query_paging());
  TEST(test_ssl());
  TEST(test_stream_storage());
  TEST(test_query_query_value());
  return 0;
}
