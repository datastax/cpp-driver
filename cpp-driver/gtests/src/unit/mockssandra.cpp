#include "mockssandra.hpp"

#include <assert.h>
#include <stdio.h>

#include "scoped_lock.hpp"

using cass::ScopedMutex;

#define SSL_BUF_SIZE 8192

namespace mockssandra {

String Ssl::generate_key() {
  EVP_PKEY* pkey = NULL;
  EVP_PKEY_CTX* pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, NULL);
  EVP_PKEY_keygen_init(pctx);
  EVP_PKEY_CTX_set_rsa_keygen_bits(pctx, 2048);
  EVP_PKEY_keygen(pctx, &pkey);

  String result;
  BIO* bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(bio, pkey, NULL, NULL, 0, NULL, NULL);
  BUF_MEM* mem = NULL;
  BIO_get_mem_ptr(bio, &mem);
  result.append(mem->data, mem->length);
  BIO_free(bio);

  EVP_PKEY_free(pkey);

  return result;
}

String Ssl::gererate_cert(const String& key, const String cn) {
  EVP_PKEY* pkey = NULL;
  { // Read key from string
    BIO* bio = BIO_new_mem_buf(key.c_str(), key.length());
    if (!PEM_read_bio_PrivateKey(bio, &pkey, NULL, NULL)) {
      BIO_free(bio);
      return "";
    }
    BIO_free(bio);
  }

  X509* x509 = X509_new();
  X509_set_version(x509, 2);
  ASN1_INTEGER_set(X509_get_serialNumber(x509), 0);
  X509_gmtime_adj(X509_get_notBefore(x509), 0);
  X509_gmtime_adj(X509_get_notAfter(x509), (long)60*60*24*365);
  X509_set_pubkey(x509, pkey);

  X509_NAME* name = X509_get_subject_name(x509);
  X509_NAME_add_entry_by_txt(name, "C", MBSTRING_ASC, (const unsigned char*)"US", -1, -1, 0);
  X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC, (const unsigned char*)cn.c_str(), -1, -1, 0);
  X509_set_issuer_name(x509, name);
  X509_sign(x509, pkey, EVP_md5());

  String result;
  { // Write cert into string
    BIO* bio = BIO_new(BIO_s_mem());
    PEM_write_bio_X509(bio, x509);
    BUF_MEM* mem = NULL;
    BIO_get_mem_ptr(bio, &mem);
    result.append(mem->data, mem->length);
    BIO_free(bio);
  }

  X509_free(x509);
  EVP_PKEY_free(pkey);

  return result;
}

namespace internal {

static void print_ssl_error() {
  unsigned long n = ERR_get_error();
  char buf[1024];
  fprintf(stderr, "%s\n", ERR_error_string(n, buf));
}

struct WriteReq {
  WriteReq(const char* data, size_t len, ClientConnection* connection)
    : data(data, len)
    , connection(connection) {
    req.data = this;
  }
  const String data;
  ClientConnection* const connection;
  uv_write_t req;
};

Tcp::Tcp(void* data) {
  tcp_.data = data;
}

int Tcp::init(uv_loop_t* loop) {
  return uv_tcp_init(loop, &tcp_);
}

int Tcp::bind(const struct sockaddr* addr) {
  return uv_tcp_bind(&tcp_, addr, 0);
}

uv_handle_t* Tcp::as_handle() {
  return reinterpret_cast<uv_handle_t*>(&tcp_);
}

uv_stream_t* Tcp::as_stream() {
  return reinterpret_cast<uv_stream_t*>(&tcp_);
}

ClientConnection::ClientConnection(ServerConnection* server)
  : tcp_(this)
  , server_(server)
  , ssl_(server->ssl_context() ? SSL_new(server->ssl_context()) : NULL)
  , incoming_bio_(ssl_ ? BIO_new(BIO_s_mem()) : NULL)
  , outgoing_bio_(ssl_ ? BIO_new(BIO_s_mem()) : NULL)
  , handshake_state_(SSL_HANDSHAKE_INPROGRESS) {
  tcp_.init(server->loop());
  if (ssl_) {
    SSL_set_bio(ssl_, incoming_bio_, outgoing_bio_);
  }
}

ClientConnection::~ClientConnection() {
  if (ssl_) SSL_free(ssl_);
}

int ClientConnection::write(const String& data) {
  return write(data.data(), data.length());
}

int ClientConnection::write(const char* data, size_t len) {
  if (ssl_) {
    return ssl_write(data, len);
  } else {
    return internal_write(data, len);
  }
}

void ClientConnection::close() {
  if (!uv_is_closing(tcp_.as_handle())) {
    uv_close(tcp_.as_handle(), on_close);
  }
}

int ClientConnection::accept() {
  int rc = server_->accept(tcp_.as_stream());
  if (rc != 0) return rc;
  return uv_read_start(tcp_.as_stream(), on_alloc, on_read);
}

void ClientConnection::on_close(uv_handle_t* handle) {
  ClientConnection* connection = static_cast<ClientConnection*>(handle->data);
  connection->handle_close();
}

void ClientConnection::handle_close() {
  on_close();
  server_->remove(this);
  Memory::deallocate(this);
}

void ClientConnection::on_alloc(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  buf->base = static_cast<char*>(Memory::malloc(suggested_size));
  buf->len = suggested_size;
}

void ClientConnection::on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  ClientConnection* connection = static_cast<ClientConnection*>(stream->data);
  connection->handle_read(nread, buf);
  Memory::free(buf->base);
}

void ClientConnection::handle_read(ssize_t nread, const uv_buf_t* buf) {
  if (nread < 0) {
    if (nread != UV_EOF && nread != UV_ECONNRESET) {
      fprintf(stderr, "Read failure: %s", uv_strerror(nread));
    }
    close();
    return;
  }
  if (ssl_) {
    on_ssl_read(buf->base, nread);
  } else {
    on_read(buf->base, nread);
  }
}

void ClientConnection::on_write(uv_write_t* req, int status) {
  WriteReq* write = static_cast<WriteReq*>(req->data);
  write->connection->handle_write(status);
  Memory::deallocate(write);
}

void ClientConnection::handle_write(int status) {
  if (status != 0) {
    fprintf(stderr, "Write failure: %s", uv_strerror(status));
    close();
    return;
  }
  if (ssl_) {
    switch (handshake_state_) {
      case SSL_HANDSHAKE_INPROGRESS:
        // Nothing to do
        break;
      case SSL_HANDSHAKE_DONE:
        on_write();
        break;
      case SSL_HANDSHAKE_FINAL_WRITE:
        handshake_state_ = SSL_HANDSHAKE_DONE;
        break;
    }
  } else {
    on_write();
  }
}

int ClientConnection::internal_write(const char* data, size_t len) {
  uv_buf_t buf;
  WriteReq* write = Memory::allocate<WriteReq>(data, len, this);
  buf.base = const_cast<char*>(write->data.data());
  buf.len = write->data.length();
  int rc = uv_write(&write->req, tcp_.as_stream(), &buf, 1, on_write);
  if (rc != 0) {
    Memory::deallocate(write);
  }
  return rc;
}

int ClientConnection::ssl_write(const char* data, size_t len) {
  if (has_ssl_error(SSL_write(ssl_, data, len))) {
    return -1;
  }

  char buf[SSL_BUF_SIZE];
  int num_bytes;
  while ((num_bytes = BIO_read(outgoing_bio_, buf, sizeof(buf))) > 0) {
    int rc = internal_write(buf, num_bytes);
    if (rc != 0) {
      return rc;
    }
  }

  return 0;
}

bool ClientConnection::is_handshake_done() {
  return SSL_is_init_finished(ssl_) != 0;
}

bool ClientConnection::has_ssl_error(int rc) {
  if (rc > 0) return false;

  int err = SSL_get_error(ssl_, rc);
  if (err == SSL_ERROR_ZERO_RETURN) {
    close();
  } else if (err != SSL_ERROR_WANT_READ && err != SSL_ERROR_NONE) {
    const char* data;
    int flags;
    int err;
    String error;
    while ((err = ERR_get_error_line_data(NULL, NULL, &data, &flags)) != 0) {
      char buf[256];
      ERR_error_string_n(err, buf, sizeof(buf));
      if (!error.empty()) error.push_back(',');
      error.append(buf);
      if (flags & ERR_TXT_STRING) {
        error.push_back(':');
        error.append(data);
      }
    }
    fprintf(stderr, "SSL error: %s\n", error.c_str());
    close();
    return true;
  }

  return false;
}

void ClientConnection::on_ssl_read(const char* data, size_t len) {
  int rc;
  BIO_write(incoming_bio_, data, len);

  if (!is_handshake_done()) {
    int rc = SSL_accept(ssl_);
    if (has_ssl_error(rc)) {
      return;
    }

    char buf[SSL_BUF_SIZE];
    bool data_written = false;
    int num_bytes;
    while ((num_bytes = BIO_read(outgoing_bio_, buf, sizeof(buf))) > 0) {
      data_written = true;
      internal_write(buf, num_bytes);
    }

    if (is_handshake_done()) {
      handshake_state_ = data_written ? SSL_HANDSHAKE_FINAL_WRITE : SSL_HANDSHAKE_DONE;
    }
  }

  if (is_handshake_done()) {
    char buf[SSL_BUF_SIZE];
    while ((rc = SSL_read(ssl_, buf, sizeof(buf))) > 0) {
      on_read(buf, rc);
    }
    has_ssl_error(rc);
  }
}

ServerConnection::ServerConnection(const ClientConnectionFactory& factory)
  : tcp_(this)
  , event_loop_(NULL)
  , state_(STATE_CLOSED)
  , rc_(0)
  , factory_(factory)
  , ssl_context_(NULL) {
  uv_mutex_init(&mutex_);
  uv_cond_init(&cond_);
}

ServerConnection::~ServerConnection() {
  uv_mutex_destroy(&mutex_);
  uv_cond_init(&cond_);
  if (ssl_context_) {
    SSL_CTX_free(ssl_context_);
  }
}

bool ServerConnection::use_ssl(const String& key,
                               const String& cert,
                               const String& password) {
  if (ssl_context_) {
    SSL_CTX_free(ssl_context_);
  }

  if ((ssl_context_ = SSL_CTX_new(SSLv23_server_method())) == NULL) {
    print_ssl_error();
    return false;
  }

  SSL_CTX_set_default_passwd_cb_userdata(ssl_context_, (void*)password.c_str());
  SSL_CTX_set_default_passwd_cb(ssl_context_, on_password);

  X509* x509 = NULL;
  { // Read cert from string
    BIO* bio = BIO_new_mem_buf(cert.c_str(), cert.length());
    if (PEM_read_bio_X509(bio, &x509, NULL, NULL) == NULL) {
      print_ssl_error();
      BIO_free(bio);
      return false;
    }
    BIO_free(bio);
  }

  if (SSL_CTX_use_certificate(ssl_context_, x509) <= 0) {
    print_ssl_error();
    return false;
  }

  EVP_PKEY* pkey = NULL;
  { // Read key from string
    BIO* bio = BIO_new_mem_buf(key.c_str(), key.length());
    if (PEM_read_bio_PrivateKey(bio, &pkey, on_password, (void*)password.c_str()) == NULL) {
      print_ssl_error();
      BIO_free(bio);
      return false;
    }
    BIO_free(bio);
  }

  if (SSL_CTX_use_PrivateKey(ssl_context_, pkey) <= 0) {
    print_ssl_error();
    EVP_PKEY_free(pkey);
    return false;
  }

  RSA* rsa = RSA_generate_key(512, RSA_F4, NULL, NULL);
  SSL_CTX_set_tmp_rsa(ssl_context_, rsa);
  RSA_free(rsa);

  SSL_CTX_set_verify(ssl_context_, SSL_VERIFY_NONE, 0);

  return true;
}

using cass::Task;

class RunListen : public Task {
public:
  RunListen(ServerConnection* server, const Address& address)
    : server_(server)
    , address_(address) { }

  virtual void run(EventLoop* event_loop) {
    server_->internal_listen(address_);
  }

private:
  ServerConnection* server_;
  const Address address_;
};

class RunClose : public Task {
public:
  RunClose(ServerConnection* server)
    : server_(server) { }

  virtual void run(EventLoop* event_loop) {
    server_->internal_close();
  }

private:
  ServerConnection* server_;
};


void ServerConnection::listen(EventLoopGroup* event_loop_group, const Address& address) {
  ScopedMutex l(&mutex_);
  if (state_ != STATE_CLOSED) return;
  state_ = STATE_PENDING;
  event_loop_ = event_loop_group->add(Memory::allocate<RunListen>(this, address));
}

int ServerConnection::wait_listen() {
  ScopedMutex l(&mutex_);
  while (state_ == STATE_PENDING) {
    uv_cond_wait(&cond_, l.get());
  }
  return rc_;
}

void ServerConnection::close() {
  ScopedMutex l(&mutex_);
  if (state_ != STATE_LISTENING && state_ != STATE_PENDING) return;
  state_ = STATE_CLOSING;
  event_loop_->add(Memory::allocate<RunClose>(this));
}

void ServerConnection::wait_close() {
  ScopedMutex l(&mutex_);
  while (state_ == STATE_CLOSING) {
    uv_cond_wait(&cond_, l.get());
  }
}

void ServerConnection::internal_listen(const Address& address) {
  int rc = 0;

  { // Locked
    ScopedMutex l(&mutex_);
    rc = tcp_.init(loop());
    if (rc != 0) {
      fprintf(stderr, "Unable to initialize socket\n");
      signal_listen(rc);
      return;
    }

    rc = tcp_.bind(address.addr());
    if (rc != 0) {
      fprintf(stderr, "Unable to bind address %s\n", address.to_string().c_str());
      signal_listen(rc);
      return;
    }

    rc = uv_listen(tcp_.as_stream(), 128, on_connection);
    if (rc != 0) {
      fprintf(stderr, "Unable to listen on address %s\n", address.to_string().c_str());
      signal_listen(rc);
      return;
    }
  }

  inc_ref();
  signal_listen(rc);
}

int ServerConnection::accept(uv_stream_t* client) {
  return uv_accept(tcp_.as_stream(), client);
}

void ServerConnection::remove(ClientConnection* connection) {
  connections_.erase(std::remove(connections_.begin(), connections_.end(), connection),
                     connections_.end());
  maybe_close();
}

void ServerConnection::internal_close() {
  for (Vec::iterator it = connections_.begin(),
       end = connections_.end();
       it != end; ++it) {
    (*it)->close();
  }
  maybe_close();
}

void ServerConnection::maybe_close() {
  ScopedMutex l(&mutex_);
  if (state_ == STATE_CLOSING &&
      connections_.empty() &&
      !uv_is_closing(tcp_.as_handle())) {
    uv_close(tcp_.as_handle(), on_close);
  }
}

void ServerConnection::signal_listen(int rc) {
  ScopedMutex l(&mutex_);
  if (rc != 0) {
    rc_ = rc;
    state_ = STATE_ERROR;
  } else {
    state_ = STATE_LISTENING;
  }
  uv_cond_signal(&cond_);
}

void ServerConnection::signal_close() {
  ScopedMutex l(&mutex_);
  event_loop_ = NULL;
  state_ = STATE_CLOSED;
  rc_ = 0;
  uv_cond_signal(&cond_);
}

void ServerConnection::on_connection(uv_stream_t* server, int status) {
  ServerConnection* self = static_cast<ServerConnection*>(server->data);
  self->handle_connection(status);
}

void ServerConnection::handle_connection(int status) {
  if (status != 0) {
    fprintf(stderr, "Listen failure: %s", uv_strerror(status));
    return;
  }

  ClientConnection* connection = factory_.create(this);
  if (connection && connection->on_accept() != 0) {
    Memory::deallocate(connection);
    return;
  }
  connections_.push_back(connection);
}

void ServerConnection::on_close(uv_handle_t* handle) {
  ServerConnection* self = static_cast<ServerConnection*>(handle->data);
  self->handle_close();
}

void ServerConnection::handle_close() {
  signal_close();
  dec_ref();
}

int ServerConnection::on_password(char* buf, int size, int rwflag, void* password) {
  strncpy(buf, (char *)(password), size);
  buf[size - 1] = '\0';
  return strlen(buf);
}

} // namespace internal

#define CHECK(pos, error) do { \
  if ((pos) > end) { \
    fprintf(stderr, "Decoding error: %s\n", (error)); \
    return end + 1; \
   } \
} while(0)

inline const char* decode_int8(const char* input, const char* end, int8_t* value) {
  CHECK(input + 1, "Unable to decode byte");
  *value = static_cast<int8_t>(input[0]);
  return input + sizeof(int8_t);
}

inline const char* decode_int16(const char* input, const char* end, int16_t* value) {
  CHECK(input + 2, "Unable to decode signed short");
  *value = (static_cast<int16_t>(static_cast<uint8_t>(input[1])) << 0) |
      (static_cast<int16_t>(static_cast<uint8_t>(input[0])) << 8);
  return input + sizeof(int16_t);
}

inline const char* decode_uint16(const char* input, const char* end, uint16_t* value) {
  CHECK(input + 2, "Unable to decode unsigned short");
  *value = (static_cast<uint16_t>(static_cast<uint8_t>(input[1])) << 0) |
      (static_cast<uint16_t>(static_cast<uint8_t>(input[0])) << 8);
  return input + sizeof(uint16_t);
}

inline const char* decode_int32(const char* input, const char* end, int32_t* value) {
  CHECK(input + 4, "Unable to decode integer");
  *value = (static_cast<int32_t>(static_cast<uint8_t>(input[3])) << 0) |
      (static_cast<int32_t>(static_cast<uint8_t>(input[2])) << 8) |
      (static_cast<int32_t>(static_cast<uint8_t>(input[1])) << 16) |
      (static_cast<int32_t>(static_cast<uint8_t>(input[0])) << 24);
  return input + sizeof(int32_t);
}

inline const char* decode_int64(const char* input, const char* end, int64_t* value) {
  CHECK(input + 8, "Unable to decode long");
  *value = (static_cast<int64_t>(static_cast<uint8_t>(input[7])) << 0) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[6])) << 8) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[5])) << 16) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[4])) << 24) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[3])) << 32) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[2])) << 40) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[1])) << 48) |
      (static_cast<int64_t>(static_cast<uint8_t>(input[0])) << 56);
  return input + sizeof(int64_t);
}

inline const char* decode_string(const char* input, const char* end, String* output) {
  uint16_t len = 0;
  const char* pos = decode_uint16(input, end, &len);
  CHECK(pos + len, "Unable to decode string");
  output->assign(pos, len);
  return pos + len;
}

inline const char* decode_long_string(const char* input, const char* end, String* output) {
  int32_t len = 0;
  const char* pos = decode_int32(input, end, &len);
  CHECK(pos + len, "Unable to decode long string");
  assert(len >= 0);
  output->assign(pos, len);
  return pos + len;
}

inline const char* decode_bytes(const char* input, const char* end, String* output) {
  int32_t len = 0;
  const char* pos = decode_int32(input, end, &len);
  if (len > 0) {
    CHECK(pos + len, "Unable to decode bytes");
    output->assign(pos, len);
  }
  return pos + len;
}

inline const char* decode_string_map(const char* input,
                                     const char* end,
                                     Vector<std::pair<String, String> >* output) {

  uint16_t len = 0;
  const char* pos = decode_uint16(input, end, &len);
  output->reserve(len);
  for (int i = 0; i < len; ++i) {
    String key;
    String value;
    pos = decode_string(pos, end, &key);
    pos = decode_string(pos, end, &value);
    output->push_back(std::pair<String, String>(key, value));
  }
  return pos;
}

inline const char* decode_stringlist(const char* input,
                                     const char* end,
                                     Vector<String>* output) {
  uint16_t len = 0;
  const char* pos = decode_uint16(input, end, &len);
  output->reserve(len);
  for (int i = 0; i < len; ++i) {
    String value;
    pos = decode_string(pos, end, &value);
    output->push_back(value);
  }
  return pos;
}

inline const char* decode_values(const char* input,
                                 const char* end,
                                 Vector<String>* output) {
  uint16_t len = 0;
  const char* pos = decode_uint16(input, end, &len);
  output->reserve(len);
  for (int i = 0; i < len; ++i) {
    String value;
    pos = decode_bytes(pos, end, &value);
    output->push_back(value);
  }
  return pos;
}

inline const char* decode_values_with_names(const char* input,
                                            const char* end,
                                            Vector<String>* names,
                                            Vector<String>* values) {
  uint16_t len = 0;
  const char* pos = decode_uint16(input, end, &len);
  names->reserve(len);
  values->reserve(len);
  for (int i = 0; i < len; ++i) {
    String name;
    pos = decode_string(pos, end, &name);
    names->push_back(name);
    String value;
    pos = decode_bytes(pos, end, &value);
    values->push_back(value);
  }
  return pos;
}

const char* decode_query_params_v1(const char* input, const char* end,
                                   bool is_execute, QueryParameters* params) {
  const char* pos = input;
  if (is_execute) {
    pos = decode_values(pos, end, &params->values);
    pos = decode_uint16(pos, end, &params->consistency);
  } else {
    pos = decode_uint16(pos, end, &params->consistency);
  }
  return pos;
}

const char* decode_query_params_v2(const char* input, const char*end,
                                   QueryParameters* params) {
  int8_t flags;
  const char* pos = input;
  pos = decode_uint16(pos, end, &params->consistency);
  pos = decode_int8(pos, end, &flags);
  params->flags = flags;
  if (flags & QUERY_FLAG_VALUES) {
    pos = decode_values(pos, end, &params->values);
  }
  if (flags & QUERY_FLAG_PAGE_SIZE) {
    pos = decode_int32(pos, end, &params->result_page_size);
  }
  if (flags & QUERY_FLAG_PAGE_STATE) {
    pos = decode_bytes(pos, end, &params->paging_state);
  }
  if (flags & QUERY_FLAG_SERIAL_CONSISTENCY) {
    pos = decode_uint16(pos, end, &params->serial_consistency);
  }
  return pos;
}

const char* decode_query_params_v3v4(const char* input, const char* end,
                                     QueryParameters* params) {
  int8_t flags;
  const char* pos = input;
  pos = decode_uint16(pos, end, &params->consistency);
  pos = decode_int8(pos, end, &flags);
  params->flags = flags;
  if (flags & QUERY_FLAG_VALUES && flags & QUERY_FLAG_NAMES_FOR_VALUES) {
    pos = decode_values_with_names(pos, end, &params->names, &params->values);
  } else if (flags & QUERY_FLAG_VALUES) {
    pos = decode_values(pos, end, &params->values);
  }
  if (flags & QUERY_FLAG_PAGE_SIZE) {
    pos = decode_int32(pos, end, &params->result_page_size);
  }
  if (flags & QUERY_FLAG_PAGE_STATE) {
    pos = decode_bytes(pos, end, &params->paging_state);
  }
  if (flags & QUERY_FLAG_SERIAL_CONSISTENCY) {
    pos = decode_uint16(pos, end, &params->serial_consistency);
  }
  if (flags & QUERY_FLAG_TIMESTAMP) {
    pos = decode_int64(pos, end, &params->timestamp);
  }
  return pos;
}

const char* decode_query_params_v5(const char* input, const char* end,
                                   QueryParameters* params) {
  int32_t flags;
  const char* pos = input;
  pos = decode_uint16(pos, end, &params->consistency);
  pos = decode_int32(pos, end, &flags);
  params->flags = flags;
  if (flags & QUERY_FLAG_VALUES && flags & QUERY_FLAG_NAMES_FOR_VALUES) {
    pos = decode_values_with_names(pos, end, &params->names, &params->values);
  } else if (flags & QUERY_FLAG_VALUES) {
    pos = decode_values(pos, end, &params->values);
  }
  if (flags & QUERY_FLAG_PAGE_SIZE) {
    pos = decode_int32(pos, end, &params->result_page_size);
  }
  if (flags & QUERY_FLAG_PAGE_STATE) {
    pos = decode_bytes(pos, end, &params->paging_state);
  }
  if (flags & QUERY_FLAG_SERIAL_CONSISTENCY) {
    pos = decode_uint16(pos, end, &params->serial_consistency);
  }
  if (flags & QUERY_FLAG_TIMESTAMP) {
    pos = decode_int64(pos, end, &params->timestamp);
  }
  if (flags & QUERY_FLAG_KEYSPACE) {
    pos = decode_string(pos, end, &params->keyspace);
  }
  return pos;
}

const char* decode_query_params(int version, const char* input, const char* end,
                                bool is_execute, QueryParameters* params) {
  const char* pos = input;
  if (version == 1) {
    pos = decode_query_params_v1(pos, end, is_execute, params);
  } else if (version == 2) {
    pos = decode_query_params_v2(pos, end, params);
  } else if (version == 3 || version == 4) {
    pos = decode_query_params_v3v4(pos, end, params);
  } else if (version == 5) {
    pos = decode_query_params_v5(pos, end, params);
  } else {
    assert(false && "Unsupported protocol version");
  }
  return pos;
}

const char* decode_prepare_params(int version, const char* input, const char* end,
                                  PrepareParameters* params) {
  const char* pos = input;
  if (version >= 5) {
    pos = decode_int32(pos, end, &params->flags);
    if (params->flags & PREPARE_FLAGS_KEYSPACE) {
      pos = decode_string(pos, end, &params->keyspace);
    }
  }
  return pos;
}


inline int32_t encode_int8(int8_t value, String* output) {
  output->push_back(static_cast<char>(value));
  return 1;
}

inline int32_t encode_int16(int16_t value, String* output) {
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 0));
  return 2;
}

inline int32_t encode_uint16(uint16_t value, String* output) {
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 0));
  return 2;
}

inline int32_t encode_int32(int32_t value, String* output) {
  output->push_back(static_cast<char>(value >> 24));
  output->push_back(static_cast<char>(value >> 16));
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value >> 0));
  return 4;
}

inline int32_t encode_string(const String& value, String* output) {
  int32_t size = encode_uint16(value.size(), output) + value.size();
  output->append(value);
  return size;
}

inline int32_t encode_bytes(const String& value, String* output) {
  int32_t size = encode_int32(value.size(), output) + value.size();
  output->append(value);
  return size;
}

Action::Builder& Action::Builder::execute(Action* action) {
  action_.reset(action);
  return builder();
}

Action::Builder& Action::Builder::nop() {
  return execute(Memory::allocate<Nop>());
}

Action::Builder& Action::Builder::wait(uint64_t timeout) {
  return execute(Memory::allocate<Wait>(timeout));
}

Action::Builder& Action::Builder::close() {
  return execute(Memory::allocate<Close>());
}

Action::Builder& Action::Builder::error(int32_t code, const String& message) {
  return execute(Memory::allocate<SendError>(code, message));
}

Action::Builder& Action::Builder::ready() {
  return execute(Memory::allocate<SendReady>());
}

Action::Builder& Action::Builder::authenticate(const String& class_name) {
  return execute(Memory::allocate<SendAuthenticate>(class_name));
}

Action::Builder& Action::Builder::auth_challenge(const String& token) {
  return execute(Memory::allocate<SendAuthChallenge>(token));
}

Action::Builder& Action::Builder::auth_success(const String& token) {
  return execute(Memory::allocate<SendAuthSuccess>(token));
}

Action::Builder& Action::Builder::supported() {
  return execute(Memory::allocate<SendSupported>());
}

Action::Builder& Action::Builder::void_result() {
  return execute(Memory::allocate<VoidResult>());
}

Action::Builder& Action::Builder::no_result() {
  return execute(Memory::allocate<NoResult>());
}

Action::Builder& Action::Builder::use_keyspace(const cass::String& keyspace) {
  return execute((Memory::allocate<UseKeyspace>(keyspace)));
}

Action::Builder& Action::Builder::plaintext_auth(const cass::String& username,
                                                const cass::String& password) {
  return execute((Memory::allocate<PlaintextAuth>(username, password)));
}

Action::Builder& Action::Builder::validate_startup() {
  return execute(Memory::allocate<ValidateStartup>());
}

Action::Builder& Action::Builder::validate_credentials() {
  return execute(Memory::allocate<ValidateCredentials>());
}

Action::Builder& Action::Builder::validate_auth_response() {
  return execute(Memory::allocate<ValidateAuthResponse>());
}

Action::Builder& Action::Builder::validate_register() {
  return execute(Memory::allocate<ValidateRegister>());
}

Action::Builder& Action::Builder::validate_query() {
  return execute(Memory::allocate<ValidateQuery>());
}

const Action* Action::Builder::build() {
  if (action_ && builder_) {
    action_->next = builder_->build();
  }
  return action_.release();
}

Action::Builder& Action::Builder::builder() {
  if (!builder_) {
    builder_.reset(Memory::allocate<Builder>());
  }
  return *builder_;
}

void Action::run(Request* request) const {
  if (on_run(request)) {
    Memory::deallocate(request);
  }
}

void Action::run_next(Request* request) const {
  if (next) {
    next->run(request);
  } else {
    Memory::deallocate(request);
  }
}

Request::Request(int8_t version, int8_t flags, int16_t stream, int8_t opcode,
                 const String& body, ClientConnection* client)
  : version_(version)
  , flags_(flags)
  , stream_(stream)
  , opcode_(opcode)
  , body_(body)
  , client_(client)
  , timer_action_(NULL) {
  client->add(this);
  (void)flags_; // TODO: Implement custom playload etc.
}

Request::~Request() {
  client_->remove(this);
}

void Request::write(int8_t opcode, const String& body) {
  client_->write(encode_header(opcode, body.length()) + body);
}

void Request::error(int32_t code, const String& message) {
  String body;
  encode_int32(code, &body);
  encode_string(message, &body);
  write(OPCODE_ERROR, body);
}

void Request::wait(uint64_t timeout, const Action* action) {
  timer_action_ = action;
  timer_.start(client_->server()->loop(), timeout, this, on_timeout);
}

void Request::close() {
  client_->close();
}

bool Request::decode_startup(Options* options) {
  return decode_string_map(start(), end(), options) == end();
}

bool Request::decode_credentials(Credentials* creds) {
  return decode_string_map(start(), end(), creds) == end();
}

bool Request::decode_auth_response(String* token) {
  return decode_bytes(start(), end(), token) == end();
}

bool Request::decode_register(EventTypes* types) {
  return decode_stringlist(start(), end(), types) == end();
}

bool Request::decode_query(String* query, QueryParameters* params) {
  return decode_query_params(version_, decode_long_string(start(), end(), query), end(), false, params) == end();
}

bool Request::decode_execute(String* id, QueryParameters* params) {
  return decode_query_params(version_, decode_string(start(), end(), id), end(), true, params) == end();
}

bool Request::decode_prepare(String* query, PrepareParameters* params) {
  return decode_prepare_params(version_, decode_long_string(start(), end(), query), end(), params) == end();
}

String Request::encode_header(int8_t opcode, int32_t len) {
  String header;
  encode_int8(0x80 | version_, &header);
  encode_int8(0, &header);
  if (version_ >= 3) {
    encode_int16(stream_, &header);
  } else {
    encode_int8(stream_, &header);
  }
  encode_int8(opcode, &header);
  encode_int32(len, &header);
  return header;
}

void Request::on_timeout(Timer* timer) {
  Request* request = static_cast<Request*>(timer->data());
  request->timer_action_->run_next(request);
}

bool SendError::on_run(Request* request) const {
  request->error(code, message);
  return true;
}

bool SendReady::on_run(Request* request) const {
  request->write(OPCODE_READY, String());
  return true;
}

bool SendAuthenticate::on_run(Request* request) const {
  String body;
  encode_string(class_name, &body);
  request->write(OPCODE_AUTHENTICATE, body);
  return true;
}

bool SendAuthChallenge::on_run(Request* request) const {
  String body;
  encode_string(token, &body);
  request->write(OPCODE_AUTH_CHALLENGE, body);
  return true;
}

bool SendAuthSuccess::on_run(Request* request) const {
  String body;
  encode_string(token, &body);
  request->write(OPCODE_AUTH_SUCCESS, body);
  return true;
}

bool SendSupported::on_run(Request* request) const {
  String body;
  encode_uint16(0, &body);
  request->write(OPCODE_SUPPORTED, body);
  return true;
}

bool VoidResult::on_run(Request* request) const {
  String body;
  encode_int32(RESULT_VOID, &body);
  request->write(OPCODE_RESULT, body);
  return true;
}

bool NoResult::on_run(Request* request) const {
  return true;
}

bool UseKeyspace::on_run(Request* request) const {
  String query;
  QueryParameters params;
  if (request->decode_query(&query, &params)) {
    query.erase(0, query.find_first_not_of(" \t"));
    if (query.substr(0, 3) == "USE" || query.substr(0, 3) == "use") {
      query.erase(0, 3);
      query.erase(0, query.find_first_not_of(" \t"));
      if (query.substr(0, keyspace.size()) == keyspace) {
        String body;
        encode_int32(RESULT_SET_KEYSPACE, &body);
        encode_string(keyspace, &body);
        request->write(OPCODE_RESULT, body);
      } else {
        request->error(ERROR_INVALID_QUERY, "Keyspace '" + keyspace + "' does not exist");
      }
      return true;
    } else {
      run_next(request);
      return false;
    }
  } else {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid query message");
    return true;
  }
}

bool PlaintextAuth::on_run(Request* request) const {
  String token;
  if (request->decode_auth_response(&token)) {
    String username, password;
    String::const_reverse_iterator last = token.rbegin();
    enum { PASSWORD, USERNAME } state = PASSWORD;
    for (String::const_reverse_iterator it = token.rbegin(),
         end = token.rend(); it != end; ++it) {
      if (*it == '\0') {
        if (state == PASSWORD) {
          password.assign(it.base(), last.base());
          state = USERNAME;
        } else if (state == USERNAME) {
          username.assign(it.base(), last.base());
          break;
        }
        last = it + 1;
      }
    }

    if (username == this->username && password == this->password) {
      String body;
      encode_int32(-1, &body); // Null bytes
      request->write(OPCODE_AUTH_SUCCESS, body);
    } else {
      request->error(ERROR_BAD_CREDENTIALS, "Invalid credentials");
    }
  } else {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid auth response message");
  }
  return true;
}

bool MatchQuery::on_run(Request* request) const {
  return false;
}

bool ValidateStartup::on_run(Request* request) const {
  Options options;
  if (!request->decode_startup(&options)) {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid startup message");
    return true;
  } else {
    run_next(request);
    return false;
  }
}

bool ValidateCredentials::on_run(Request* request) const {
  Credentials creds;
  if (!request->decode_credentials(&creds)) {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid credentials message");
    return true;
  } else {
    run_next(request);
    return false;
  }
}

bool ValidateAuthResponse::on_run(Request* request) const {
  String token;
  if (!request->decode_auth_response(&token)) {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid auth response message");
    return true;
  } else {
    run_next(request);
    return false;
  }
}

bool ValidateRegister::on_run(Request* request) const {
  EventTypes types;
  if (!request->decode_register(&types)) {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid register message");
    return true;
  } else {
    run_next(request);
    return false;
  }
}

bool ValidateQuery::on_run(Request* request) const {
  String query;
  QueryParameters params;
  if (!request->decode_query(&query, &params)) {
    request->error(ERROR_PROTOCOL_ERROR, "Invalid query message");
    return true;
  } else {
    run_next(request);
    return false;
  }
}

RequestHandler::RequestHandler(RequestHandler::Builder* builder)
  : invalid_protocol_(builder->on_invalid_protocol().build())
  , invalid_opcode_(builder->on_invalid_protocol().build()) {
  actions_[OPCODE_STARTUP].reset(builder->on(OPCODE_STARTUP).build());
  actions_[OPCODE_OPTIONS].reset(builder->on(OPCODE_OPTIONS).build());
  actions_[OPCODE_CREDENTIALS].reset(builder->on(OPCODE_CREDENTIALS).build());
  actions_[OPCODE_QUERY].reset(builder->on(OPCODE_QUERY).build());
  actions_[OPCODE_PREPARE].reset(builder->on(OPCODE_PREPARE).build());
  actions_[OPCODE_EXECUTE].reset(builder->on(OPCODE_EXECUTE).build());
  actions_[OPCODE_REGISTER].reset(builder->on(OPCODE_REGISTER).build());
  actions_[OPCODE_AUTH_RESPONSE].reset(builder->on(OPCODE_AUTH_RESPONSE).build());
}

const RequestHandler* RequestHandler::Builder::build() {
  return Memory::allocate<RequestHandler>(this);
}

void ProtocolHandler::decode(ClientConnection* client, const char* data, int32_t len) {
  buffer_.append(data, len);
  int32_t result = decode_frame(client, buffer_.data(), buffer_.size());
  if (result > 0) {
    if (static_cast<size_t>(result) == buffer_.size()) {
      buffer_.clear();
    } else {
      // Not efficient, but concise. Copy the consumed part of the buffer
      // forward then resize the buffer to what's left over.
      std::copy(buffer_.begin(), buffer_.begin() + result, buffer_.end());
      buffer_.resize(buffer_.size() - result);
    }
  }
}

int32_t ProtocolHandler::decode_frame(ClientConnection* client, const char* frame, int32_t len) {
  const char* pos = frame;
  const char* end = pos + len;
  int32_t remaining = len;

  while (remaining > 0) {
    switch (state_) {
      case PROTOCOL_VERSION:
        // Version requires a single byte and that's guaranteed by the loop check.
        version_ = *pos++;
        remaining--;
        if (version_ < 1 || version_ > 5) {
          request_handler_->invalid_protocol(Memory::allocate<Request>(version_, flags_, stream_, opcode_, String(), client));
          return len - remaining;
        }
        state_ = HEADER;
        break;
      case HEADER:
        if ((version_ == 1 || version_ == 2) && remaining >= 7) {
          flags_ = *pos++;
          stream_ = *pos++;
          opcode_ = *pos++;
          pos = decode_int32(pos, end, &length_);
          remaining -= 7;
        } else if (version_ >= 3 && version_ <= 5 && remaining >= 8) {
          flags_ = *pos++;
          pos = decode_int16(pos, end, &stream_);
          opcode_ = *pos++;
          pos = decode_int32(pos, end, &length_);
          remaining -= 8;
        } else {
          return len - remaining;
        }
        state_ = BODY;
        break;
      case BODY:
        if (remaining >= length_) {
          decode_body(client, pos, length_);
          remaining -= length_;
        } else {
          return len - remaining;
        }
        version_ = 0;
        flags_ = 0;
        opcode_ = 0;
        length_ = 0;
        state_ = PROTOCOL_VERSION;
        break;
    }
  }

  return len; // All bytes have been consumed
}

void ProtocolHandler::decode_body(ClientConnection* client, const char* body, int32_t len) {
  request_handler_->run(Memory::allocate<Request>(version_, flags_, stream_, opcode_, String(body, len), client));
}

ClientConnection::~ClientConnection() {
  while(!requests_.is_empty()) {
    Memory::deallocate(requests_.front()); // Removes itself from the list
  }
}

void ClientConnection::on_read(const char* data, size_t len) {
  handler_.decode(this, data, len);
}

void Cluster::init(AddressGenerator& generator,
                   ClientConnectionFactory& factory,
                   size_t num_nodes) {
  for (size_t i = 0; i < num_nodes; ++i) {
    Server server(generator.next(),
                  internal::ServerConnection::Ptr(
                    Memory::allocate<internal::ServerConnection>(factory)));
    servers_.push_back(server);
  }
}

Cluster::~Cluster() {
  stop_all();
}

String Cluster::use_ssl() {
  String key(Ssl::generate_key());
  String cert(Ssl::gererate_cert(key));
  for (size_t i = 0; i < servers_.size(); ++i) {
    Server& server = servers_[i];
    if (!server.connection->use_ssl(key, cert)) {
      return "";
    }
  }
  return cert;
}

int Cluster::start_all(EventLoopGroup* event_loop_group) {
  start_all_async(event_loop_group);
  for (size_t i = 0; i < servers_.size(); ++i) {
    Server& server = servers_[i];
    int rc = server.connection->wait_listen();
    if (rc != 0) return rc;
  }
  return 0;
}

void Cluster::start_all_async(cass::EventLoopGroup* event_loop_group) {
  for (size_t i = 0; i < servers_.size(); ++i) {
    Server& server = servers_[i];
    server.connection->listen(event_loop_group, server.address);
  }
}

void Cluster::stop_all() {
  stop_all_async();
  for (size_t i = 0; i < servers_.size(); ++i) {
    Server& server = servers_[i];
    server.connection->wait_close();
  }
}

void Cluster::stop_all_async() {
  for (size_t i = 0; i < servers_.size(); ++i) {
    Server& server = servers_[i];
    server.connection->close();
  }
}

int Cluster::start(EventLoopGroup* event_loop_group, size_t node) {
  if (node < 1 || node > servers_.size()) {
    return -1;
  }
  Server& server = servers_[node - 1];
  server.connection->listen(event_loop_group, server.address);
  return server.connection->wait_listen();
}

void Cluster::start_async(cass::EventLoopGroup* event_loop_group, size_t node) {
  if (node < 1 || node > servers_.size()) {
    return;
  }
  Server& server = servers_[node - 1];
  server.connection->listen(event_loop_group, server.address);
}

void Cluster::stop(size_t node) {
  if (node < 1 || node > servers_.size()) {
    return;
  }
  Server& server = servers_[node - 1];
  server.connection->close();
  server.connection->wait_close();
}

void Cluster::stop_async(size_t node) {
  if (node < 1 || node > servers_.size()) {
    return;
  }
  Server& server = servers_[node - 1];
  server.connection->close();
}

Address Ipv4AddressGenerator::next() {
  char buf[32];
  sprintf(buf, "%d.%d.%d.%d", (ip_ >> 24) & 0xff, (ip_ >> 16) & 0xff, (ip_ >> 8) & 0xff, ip_ & 0xff);
  ip_++;
  return Address(buf, port_);
}

SimpleEventLoopGroup::SimpleEventLoopGroup(size_t num_threads)
  : cass::RoundRobinEventLoopGroup(num_threads) {
  assert(init() == 0 && "Unable to initialize simple event loop");
  run();
}

SimpleEventLoopGroup::~SimpleEventLoopGroup() {
  close_handles();
  join();
}

SimpleRequestHandlerBuilder::SimpleRequestHandlerBuilder()
  : RequestHandler::Builder() {
  on(OPCODE_STARTUP).validate_startup().ready();
  on(OPCODE_OPTIONS).supported();
  on(OPCODE_REGISTER).validate_register().ready();
  on(OPCODE_CREDENTIALS).validate_credentials().ready();
  on(OPCODE_AUTH_RESPONSE).validate_auth_response().auth_success("");
  on(OPCODE_QUERY).validate_query().void_result();
}

} // namespace mockssandra
