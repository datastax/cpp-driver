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

#ifndef MOCKSSANDRA_HPP
#define MOCKSSANDRA_HPP

#include <uv.h>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>

#include <stdint.h>

#include "address.hpp"
#include "event_loop.hpp"
#include "memory.hpp"
#include "string.hpp"
#include "vector.hpp"
#include "timer.hpp"
#include "ref_counted.hpp"
#include "list.hpp"
#include "scoped_ptr.hpp"
#include "third_party/mt19937_64/mt19937_64.hpp"

#if defined(WIN32) || defined(_WIN32)
#undef ERROR_ALREADY_EXISTS
#undef ERROR
#undef X509_NAME
#endif

using cass::Address;
using cass::EventLoop;
using cass::EventLoopGroup;
using cass::List;
using cass::Memory;
using cass::String;
using cass::Task;
using cass::Timer;
using cass::Vector;
using cass::RefCounted;
using cass::SharedRefPtr;
using cass::ScopedPtr;

namespace mockssandra {

class Ssl {
public:
  static String generate_key();
  static String generate_cert(const String& key, String cn = "");
};

namespace internal {

class ServerConnection;

class Tcp {
public:
  Tcp(void* data);

  int init(uv_loop_t* loop);
  int bind(const struct sockaddr* addr);

  uv_handle_t* as_handle();
  uv_stream_t* as_stream();

private:
  uv_tcp_t tcp_;
};

class ClientConnection {
public:
  ClientConnection(ServerConnection* server);
  virtual ~ClientConnection();

  ServerConnection* server() { return server_; }

  virtual int on_accept() { return accept(); }
  virtual void on_close() { }

  virtual void on_read(const char* data, size_t len) { }
  virtual void on_write() { }

  int write(const String& data);
  int write(const char* data, size_t len);
  void close();

protected:
  int accept();

private:
  static void on_close(uv_handle_t* handle);
  void handle_close();

  static void on_alloc(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);

  static void on_read(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf);
  void handle_read(ssize_t nread, const uv_buf_t* buf);

  static void on_write(uv_write_t* req, int status);
  void handle_write(int status);

private:
  int internal_write(const char* data, size_t len);
  int ssl_write(const char* data, size_t len);

  bool is_handshake_done();
  bool has_ssl_error(int rc);

  void on_ssl_read(const char* data, size_t len);

private:
  Tcp tcp_;
  ServerConnection* const server_;

private:
  enum SslHandshakeState {
    SSL_HANDSHAKE_INPROGRESS,
    SSL_HANDSHAKE_FINAL_WRITE,
    SSL_HANDSHAKE_DONE
  };

  SSL* ssl_;
  BIO* incoming_bio_;
  BIO* outgoing_bio_;
  SslHandshakeState handshake_state_;
};

class ClientConnectionFactory {
public:
  virtual ClientConnection* create(ServerConnection* server) const = 0;
};

class ServerConnectionTask : public RefCounted<ServerConnectionTask> {
public:
  typedef SharedRefPtr<ServerConnectionTask> Ptr;

  virtual ~ServerConnectionTask() { }
  virtual void run(ServerConnection* server_connection) = 0;
};

typedef Vector<ClientConnection*> ClientConnections;

class ServerConnection : public RefCounted<ServerConnection> {
public:
  typedef SharedRefPtr<ServerConnection> Ptr;

   ServerConnection(const Address& address, const ClientConnectionFactory& factory);
   ~ServerConnection();

   const Address& address() const { return address_; }
   uv_loop_t* loop();
   SSL_CTX* ssl_context() { return ssl_context_; }
   const ClientConnections& clients() const { return clients_; }

   bool use_ssl(const String& key,
                const String& cert,
                const String& password = "");

   void listen(EventLoopGroup* event_loop_group);
   int wait_listen();

   void close();
   void wait_close();

   void run(const ServerConnectionTask::Ptr& task);

private:
   friend class ClientConnection;
   int accept(uv_stream_t* client);
   void remove(ClientConnection* connection);

private:
   friend class RunListen;
   friend class RunClose;

   void internal_listen();
   void internal_close();
   void maybe_close();

   void signal_listen(int rc);
   void signal_close();

   static void on_connection(uv_stream_t* stream, int status);
   void handle_connection(int status);

   static void on_close(uv_handle_t* handle);
   void handle_close();

   static int on_password(char *buf, int size, int rwflag, void *password);

private:
   enum State {
     STATE_CLOSED,
     STATE_CLOSING,
     STATE_ERROR,
     STATE_PENDING,
     STATE_LISTENING
   };

   Tcp tcp_;
   EventLoop* event_loop_;
   State state_;
   int rc_;
   uv_mutex_t mutex_;
   uv_cond_t cond_;
   ClientConnections clients_;
   const Address address_;
   const ClientConnectionFactory& factory_;
   SSL_CTX* ssl_context_;
};

} // namespace internal

enum {
  OPCODE_ERROR = 0x00,
  OPCODE_STARTUP = 0x01,
  OPCODE_READY = 0x02,
  OPCODE_AUTHENTICATE = 0x03,
  OPCODE_CREDENTIALS = 0x04,
  OPCODE_OPTIONS = 0x05,
  OPCODE_SUPPORTED = 0x06,
  OPCODE_QUERY = 0x07,
  OPCODE_RESULT = 0x08,
  OPCODE_PREPARE = 0x09,
  OPCODE_EXECUTE = 0x0A,
  OPCODE_REGISTER = 0x0B,
  OPCODE_EVENT = 0x0C,
  OPCODE_BATCH = 0x0D,
  OPCODE_AUTH_CHALLENGE = 0x0E,
  OPCODE_AUTH_RESPONSE = 0x0F,
  OPCODE_AUTH_SUCCESS = 0x10,
  OPCODE_LAST_ENTRY
};

enum {
  QUERY_FLAG_VALUES = 0x01,
  QUERY_FLAG_SKIP_METADATA = 0x02,
  QUERY_FLAG_PAGE_SIZE = 0x04,
  QUERY_FLAG_PAGE_STATE = 0x08,
  QUERY_FLAG_SERIAL_CONSISTENCY = 0x10,
  QUERY_FLAG_TIMESTAMP = 0x20,
  QUERY_FLAG_NAMES_FOR_VALUES = 0x40,
  QUERY_FLAG_KEYSPACE = 0x80
};

enum {
  PREPARE_FLAGS_KEYSPACE = 0x01
};

enum {
  ERROR_SERVER_ERROR = 0x0000,
  ERROR_PROTOCOL_ERROR = 0x000A,
  ERROR_BAD_CREDENTIALS = 0x0100,
  ERROR_UNAVAILABLE = 0x1000,
  ERROR_OVERLOADED = 0x1001,
  ERROR_IS_BOOTSTRAPPING = 0x1002,
  ERROR_TRUNCATE_ERROR = 0x1003,
  ERROR_WRITE_TIMEOUT = 0x1100,
  ERROR_READ_TIMEOUT = 0x1200,
  ERROR_READ_FAILURE = 0x1300,
  ERROR_FUNCTION_FAILURE = 0x1400,
  ERROR_WRITE_FAILURE = 0x1500,
  ERROR_SYNTAX_ERROR = 0x2000,
  ERROR_UNAUTHORIZED = 0x2100,
  ERROR_INVALID_QUERY = 0x2200,
  ERROR_CONFIG_ERROR = 0x2300,
  ERROR_ALREADY_EXISTS = 0x2400,
  ERROR_UNPREPARED = 0x2500,
  ERROR_CLIENT_WRITE_FAILURE = 0x8000
};

enum {
  RESULT_VOID = 0x0001,
  RESULT_ROWS = 0x0002,
  RESULT_SET_KEYSPACE = 0x0003,
  RESULT_SET_PREPARED = 0x0004,
  RESULT_SET_SCHEMA_CHANGE = 0x0005
};

enum {
  RESULT_FLAG_GLOBAL_TABLESPEC = 0x00000001,
  RESULT_FLAG_HAS_MORE_PAGES = 0x00000002,
  RESULT_FLAG_NO_METADATA = 0x00000004,
  RESULT_FLAG_METADATA_CHANGED = 0x00000008,
  RESULT_FLAG_CONTINUOUS_PAGING = 0x40000000,
  RESULT_FLAG_LAST_CONTINUOUS_PAGE = 0x80000000
};

enum {
  TYPE_CUSTOM = 0x0000,
  TYPE_ASCII = 0x0001,
  TYPE_BIGINT = 0x0002,
  TYPE_BLOG = 0x0003,
  TYPE_BOOLEAN = 0x0004,
  TYPE_COUNTER = 0x0005,
  TYPE_DECIMAL = 0x0006,
  TYPE_DOUBLE = 0x0007,
  TYPE_FLOAT = 0x0008,
  TYPE_INT = 0x0009,
  TYPE_TIMESTAMP = 0x000B,
  TYPE_UUIT = 0x000C,
  TYPE_VARCHAR = 0x000D,
  TYPE_VARINT = 0x000E,
  TYPE_TIMEUUD = 0x000F,
  TYPE_INET = 0x0010,
  TYPE_DATE = 0x0011,
  TYPE_TIME = 0x0012,
  TYPE_SMALLINT = 0x0013,
  TYPE_TINYINT = 0x0014,
  TYPE_LIST = 0x0020,
  TYPE_MAP = 0x0021,
  TYPE_SET = 0x0022,
  TYPE_UDT = 0x0030,
  TYPE_TUPLE = 0x0031
};

typedef std::pair<String, String> Option;
typedef Vector<Option> Options;
typedef std::pair<String, String> Credential;
typedef Vector<Credential> Credentials;
typedef Vector<String> EventTypes;
typedef Vector<String> Values;
typedef Vector<String> Names;

struct PrepareParameters {
  int32_t flags;
  String keyspace;
};

struct QueryParameters {
  uint16_t consistency;
  int32_t flags;
  Values values;
  Names names;
  int32_t result_page_size;
  String paging_state;
  uint16_t serial_consistency;
  int64_t timestamp;
  String keyspace;
};

class Type {
public:
  static Type text();
  static Type inet();
  static Type list(const Type& sub_type);

  void encode(int protocol_version, String* output) const;

private:
  Type()
    : type_(-1) { }

  Type(int type)
    : type_(type) { }

  friend class Vector<Type>;

private:
  int type_;
  String custom_;
  Vector<String> names_;
  Vector<Type> types_;
};

class Column {
public:
  Column(const String& name, const Type type)
    : name_(name)
    , type_(type) { }

  void encode(int protocol_version, String* output) const;

private:
  String name_;
  Type type_;
};

class ResultSet;
class Collection;

class Value {
private:
  enum Type {
    NUL,
    VALUE,
    COLLECTION
  };

public:
  Value()
    : type_(NUL) { }

  Value(const String& value)
    : type_(VALUE)
    , value_(Memory::allocate<String>(value)) { }

  Value(const Collection& collection)
    : type_(COLLECTION)
    , collection_(Memory::allocate<Collection>(collection)){ }

  Value(const Value& other)
    : type_(other.type_) {
    if (type_ == VALUE) {
      value_ = Memory::allocate<String>(*other.value_);
    } else if (type_ == COLLECTION) {
      collection_ = Memory::allocate<Collection>(*other.collection_);
    }
  }

  ~Value() {
    if (type_ == VALUE) {
      Memory::deallocate(value_);
    } else if (type_ == COLLECTION) {
      Memory::deallocate(collection_);
    }
  }

  void encode(int protocol_version, String* output) const;

private:
  Type type_;
  union {
    String* value_;
    Collection* collection_;
  };
};

class Collection {
public:
  class Builder {
  public:
    Builder(const Type& sub_type)
      : sub_type_(sub_type) { }

    Builder& text(const String& text) {
      values_.push_back(Value(text));
      return *this;
    }

    Collection build() {
      return Collection(sub_type_, values_);
    }

  private:
    Type sub_type_;
    Vector<Value> values_;
  };

  void encode(int protocol_version, String* output) const;

  static Collection text(const Vector<String>& values) {
    Collection::Builder builder(Type::text());
    for (Vector<String>::const_iterator it = values.begin(),
         end = values.end(); it != end; ++it) {
      builder.text(*it);
    }
    return builder.build();
  }

private:
  Collection(const Type& sub_type,
             const Vector<Value> values)
    : sub_type_(sub_type)
    , values_(values) { }

private:
  const Type sub_type_;
  const Vector<Value> values_;
};

class Row {
public:
  class Builder {
  public:
    Builder& text(const String& text) {
      values_.push_back(Value(text));
      return *this;
    }

    Builder& inet(const Address& inet) {
      String value;
      uint8_t buf[16];
      uint8_t len = inet.to_inet(buf);
      for (uint8_t i = 0; i < len; ++i) {
        value.push_back(static_cast<char>(buf[i]));
      }
      values_.push_back(Value(value));
      return *this;
    }

    Builder& collection(const Collection& collection) {
      values_.push_back(Value(collection));
      return *this;
    }

    Row build() const {
      return Row(values_);
    }

  private:
    Vector<Value> values_;
  };

  void encode(int protocol_version, String* output) const;

private:
  Row(const Vector<Value>& values)
    : values_(values) { }

private:
  Vector<Value> values_;
};

class ResultSet {
public:
  class Builder {
  public:
    Builder(const String& keyspace_name, const String& table_name)
      : keyspace_name_(keyspace_name)
      , table_name_(table_name) { }

    Builder& column(const String& name, const Type& type) {
      columns_.push_back(Column(name, type));
      return *this;
    }

    Builder& row(const Row& row) {
      rows_.push_back(row);
      return *this;
    }

    ResultSet build() const {
      return ResultSet(keyspace_name_, table_name_, columns_, rows_);
    }

  private:
    const String keyspace_name_;
    const String table_name_;
    Vector<Column> columns_;
    Vector<Row> rows_;
  };

  String encode(int protocol_version) const;

  size_t column_count() const { return columns_.size(); }

private:
  ResultSet(const String& keyspace_name,
            const String& table_name,
            const Vector<Column>& columns,
            const Vector<Row>& rows)
    : keyspace_name_(keyspace_name)
    , table_name_(table_name)
    , columns_(columns)
    , rows_(rows) { }

private:
  const String keyspace_name_;
  const String table_name_;
  const Vector<Column> columns_;
  const Vector<Row> rows_;
};

struct Exception : public std::exception {
  Exception(int code, const String& message)
    : code(code)
    , message(message) { }
  virtual ~Exception() throw() { }
  const int code;
  const String message;
};

struct Host {
  Host() { }
  Host(const Address& address,
       const String& dc,
       const String& rack,
       MT19937_64& token_rng,
       int num_tokens = 2);
  Address address;
  String dc;
  String rack;
  String partitioner;
  Vector<String> tokens;
};

typedef Vector<Host> Hosts;

class ClientConnection;
class Cluster;
class Request;

typedef std::pair<String, ResultSet> Match;
typedef Vector<Match> Matches;

struct Action {
  class Builder {
  public:
    Builder& execute(Action* action);
    Builder& nop();
    Builder& wait(uint64_t timeout);
    Builder& close();
    Builder& error(int32_t code, const String& message);

    Builder& ready();
    Builder& authenticate(const String& class_name);
    Builder& auth_challenge(const String& token);
    Builder& auth_success(const String& token);
    Builder& supported();
    Builder& up_event(const Address& address);

    Builder& void_result();
    Builder& empty_rows_result(int32_t row_count);
    Builder& no_result();
    Builder& match_query(const Matches& matches);

    Builder& system_local();
    Builder& system_peers();

    Builder& use_keyspace(const String& keyspace);
    Builder& plaintext_auth(const String& username,
                            const String& password);

    Builder& validate_startup();
    Builder& validate_credentials();
    Builder& validate_auth_response();
    Builder& validate_register();
    Builder& validate_query();

    Builder& set_registered_for_events();
    Builder& set_protocol_version();

    const Action* build();

  private:
    Builder& builder();

  private:
    ScopedPtr<Builder> builder_;
    ScopedPtr<Action> action_;
  };

  Action() : next(NULL) { }
  virtual ~Action() { Memory::deallocate(next); }

  void run(Request* request) const;
  void run_next(Request* request) const;

  virtual bool on_run(Request* request) const = 0;

  const Action* next;
};

class Request : public List<Request>::Node {
public:
  Request(int8_t version, int8_t flags, int16_t stream, int8_t opcode,
          const String& body, ClientConnection* client);
  ~Request();

  int8_t version() const { return version_; }
  int16_t stream() const { return stream_; }
  int8_t opcode() const { return opcode_; }

  ClientConnection* client() const { return client_; }

  void write(int8_t opcode, const String& body);
  void write(int16_t stream, int8_t opcode, const String& body);
  void error(int32_t code, const String& message);
  void wait(uint64_t timeout, const Action* action);
  void close();

  bool decode_startup(Options* options);
  bool decode_credentials(Credentials* creds);
  bool decode_auth_response(String* token);
  bool decode_register(EventTypes* types);
  bool decode_query(String* query, QueryParameters* params);
  bool decode_execute(String* id, QueryParameters* params);
  bool decode_prepare(String* query, PrepareParameters* params);

  const Address& address() const;
  const Host& host(const Address& address) const;
  Hosts hosts() const;

private:
  void on_timeout(Timer* timer);

  const char* start() { return body_.data(); }
  const char* end() { return body_.data() + body_.size(); }

private:
  const int8_t version_;
  const int8_t flags_;
  const int16_t stream_;
  const int8_t opcode_;
  const String body_;
  ClientConnection* const client_;
  Timer timer_;
  const Action* timer_action_;
};

struct Nop : public Action {
  virtual bool on_run(Request* request) const {
    return true;
  }
};

struct Wait : public Action {
  Wait(uint64_t timeout)
    : timeout(timeout) { }

  virtual bool on_run(Request* request) const {
    request->wait(timeout, this);
    return false;
  }

  const uint64_t timeout;
};

struct Close : public Action {
  virtual bool on_run(Request* request) const {
    request->close();
    return true;
  }
};

struct SendError : public Action {
  SendError(int32_t code, const String& message)
    : code(code)
    , message(message) { }

  virtual bool on_run(Request* request) const;

  int32_t code;
  String message;
};

struct SendReady : public Action {
  virtual bool on_run(Request* request) const;
};

struct SendAuthenticate : public Action {
  SendAuthenticate(const String& class_name)
    : class_name(class_name) { }
  virtual bool on_run(Request* request) const;
  String class_name;
};

struct SendAuthChallenge : public Action {
  SendAuthChallenge(const String& token)
    : token(token) { }
  virtual bool on_run(Request* request) const;
  String token;
};

struct SendAuthSuccess : public Action {
  SendAuthSuccess(const String& token)
    : token(token) { }
  virtual bool on_run(Request* request) const;
  String token;
};

struct SendSupported : public Action {
  virtual bool on_run(Request* request) const;
};

struct SendUpEvent : public Action {
  SendUpEvent(const Address& address)
    : address(address) { }
  virtual bool on_run(Request* request) const;
  Address address;
};

struct VoidResult : public Action {
  virtual bool on_run(Request* request) const;
};

struct EmptyRowsResult : public Action {
  EmptyRowsResult(int row_count)
    : row_count(row_count) { }
  virtual bool on_run(Request* request) const;
  int32_t row_count;
};

struct NoResult : public Action {
  virtual bool on_run(Request* request) const;
};

struct MatchQuery : public Action {
  MatchQuery(const Matches& matches)
    : matches(matches) { }
  virtual bool on_run(Request* request) const;
  Matches matches;
};

struct SystemLocal : public Action {
  virtual bool on_run(Request* request) const;
};

struct SystemPeers : public Action {
  virtual bool on_run(Request* request) const;
};

struct UseKeyspace : public Action {
  UseKeyspace(const String& keyspace)
    : keyspace(keyspace) { }
  virtual bool on_run(Request* request) const;
  String keyspace;
};

struct PlaintextAuth : public Action {
  PlaintextAuth (const String& username, const String& password)
    : username(username)
    , password(password) { }
  virtual bool on_run(Request* request) const;
  String username;
  String password;
};

struct ValidateStartup : public Action {
  virtual bool on_run(Request* request) const;
};

struct ValidateCredentials : public Action {
  virtual bool on_run(Request* request) const;
};

struct ValidateAuthResponse : public Action {
  virtual bool on_run(Request* request) const;
};

struct ValidateRegister : public Action {
  virtual bool on_run(Request* request) const;
};

struct ValidateQuery : public Action {
  virtual bool on_run(Request* request) const;
};

struct SetRegisteredForEvents : public Action {
  virtual bool on_run(Request* request) const;
};

struct SetProtocolVersion : public Action {
  virtual bool on_run(Request* request) const;
};

class RequestHandler {
public:
  class Builder {
  public:
    Builder()
      : lowest_supported_protocol_version_(1)
      , highest_supported_protocol_version_(5) {
      invalid_protocol_.error(ERROR_PROTOCOL_ERROR, "Invalid or unsupported protocol version");
      invalid_opcode_.error(ERROR_PROTOCOL_ERROR, "Invalid opcode (or not implemented)");
    }

    Action::Builder& on(int8_t opcode) {
      if (opcode < OPCODE_LAST_ENTRY) {
        return actions_[opcode];
      }
      return dummy_;
    }

    Action::Builder& on_invalid_protocol() { return invalid_protocol_; }
    Action::Builder& on_invalid_opcode() { return invalid_opcode_; }

    const RequestHandler* build();

    Builder& with_supported_protocol_versions(int lowest, int highest) {
      assert(highest >= lowest && "Invalid protocol versions");
      lowest_supported_protocol_version_ = lowest < 0 ? 0 : lowest;
      highest_supported_protocol_version_ = highest > 5 ? 5 : highest;
      return *this;
    }

  private:
    Action::Builder actions_[OPCODE_LAST_ENTRY];
    Action::Builder invalid_protocol_;
    Action::Builder invalid_opcode_;
    Action::Builder dummy_;
    int lowest_supported_protocol_version_;
    int highest_supported_protocol_version_;
  };

  RequestHandler(Builder* builder,
                 int lowest_supported_protocol_version,
                 int highest_supported_protocol_version);

  int lowest_supported_protocol_version() const {
    return lowest_supported_protocol_version_;
  }

  int highest_supported_protocol_version() const {
    return highest_supported_protocol_version_;
  }

  void invalid_protocol(Request* request) const {
    invalid_protocol_->run(request);
  }

  void run(Request* request) const {
    const ScopedPtr<const Action>& action = actions_[request->opcode()];
    if (action) {
      action->run(request);
    } else {
      invalid_opcode_->run(request);
    }
  }

private:
  ScopedPtr<const Action> invalid_protocol_;
  ScopedPtr<const Action> invalid_opcode_;
  ScopedPtr<const Action> actions_[OPCODE_LAST_ENTRY];
  const int lowest_supported_protocol_version_;
  const int highest_supported_protocol_version_;
};


class ProtocolHandler {
public:
  ProtocolHandler(const RequestHandler* request_handler)
    : request_handler_(request_handler)
    , state_(PROTOCOL_VERSION)
    , version_(0)
    , flags_(0)
    , stream_(0)
    , opcode_(0)
    , length_(0) { }

  void decode(ClientConnection* client, const char* data, int32_t len);

private:
  int32_t decode_frame(ClientConnection* client, const char* frame, int32_t len);
  void decode_body(ClientConnection* client, const char* body, int32_t len);

  enum State {
    PROTOCOL_VERSION,
    HEADER,
    BODY
  };

private:
  String buffer_;
  const RequestHandler* request_handler_;
  State state_;
  int8_t version_;
  int8_t flags_;
  int16_t stream_;
  int8_t opcode_;
  int32_t length_;
};

class ClientConnection : public internal::ClientConnection {
public:
  ClientConnection(internal::ServerConnection* server, const RequestHandler* request_handler, const Cluster* cluster)
    : internal::ClientConnection(server)
    , handler_(request_handler)
    , cluster_(cluster)
    , protocol_version_(-1)
    , is_registered_for_events_(false) { }

  ~ClientConnection();

  virtual void on_read(const char* data, size_t len);

  void add(Request* request) { requests_.add_to_back(request); }
  void remove(Request* request) { requests_.remove(request); }

  const Cluster* cluster() const { return cluster_; }

  int protocol_version() const { return protocol_version_; }
  void set_protocol_version(int protocol_version) { protocol_version_  = protocol_version; }

  bool is_registered_for_events() const { return is_registered_for_events_; }
  void set_registered_for_events() { is_registered_for_events_ = true; }

private:
  ProtocolHandler handler_;
  const Cluster* cluster_;
  List<Request> requests_;
  int protocol_version_;
  bool is_registered_for_events_;
};

class CloseConnection : public ClientConnection {
public:
  CloseConnection(internal::ServerConnection* server, const RequestHandler* request_handler, const Cluster* cluster)
    : ClientConnection(server, request_handler, cluster) { }

  int on_accept() {
    int rc = accept();
    if (rc != 0) {
      return rc;
    }
    close();
    return rc;
  }
};

class ClientConnectionFactory : public internal::ClientConnectionFactory {
public:
  ClientConnectionFactory(const RequestHandler* request_handler, const Cluster* cluster)
    : request_handler_(request_handler)
    , cluster_(cluster)
    , close_immediately_(false) { }

  void use_close_immediately() {
    close_immediately_ = true;
  }

  virtual internal::ClientConnection* create(internal::ServerConnection* server) const {
    if (close_immediately_) {
      return Memory::allocate<CloseConnection>(server, request_handler_.get(), cluster_);
    } else {
      return Memory::allocate<ClientConnection>(server, request_handler_.get(), cluster_);
    }
  }

private:
  ScopedPtr<const RequestHandler> request_handler_;
  const Cluster* cluster_;
  bool close_immediately_;
};

class AddressGenerator {
public:
  virtual Address next() = 0;
};

class Ipv4AddressGenerator : public AddressGenerator {
public:
  Ipv4AddressGenerator(uint8_t a = 127, uint8_t b = 0, uint8_t c = 0, uint8_t d = 1, int port = 9042)
    : ip_((a << 24) | (b << 16) | (c << 8) | (d & 0xff))
    , port_(port) { }

  virtual Address next();

private:
  uint32_t ip_;
  int port_;
};

class Event : public internal::ServerConnectionTask {
public:
  typedef SharedRefPtr<Event> Ptr;

  Event(const String& event_body);

  virtual void run(internal::ServerConnection* server_connection);

private:
  String event_body_;
};

class TopologyChangeEvent : public Event {
public:
  enum Type {
    NEW_NODE,
    MOVED_NODE,
    REMOVED_NODE
  };

  TopologyChangeEvent(Type type, const Address& address)
    : Event(encode(type, address)) { }

  static String encode(Type type, const Address& address);
};

class StatusChangeEvent : public Event {
public:
  enum Type {
    UP,
    DOWN
  };

  StatusChangeEvent(Type type, const Address& address)
    : Event(encode(type, address)) { }

  static String encode(Type type, const Address& address);
};

class SchemaChangeEvent : public Event {
public:
  enum Type {
    CREATED,
    UPDATED,
    DROPPED
  };

  enum Target {
    KEYSPACE,
    TABLE,
    USER_TYPE,
    FUNCTION,
    AGGREGATE
  };

  SchemaChangeEvent(Target target, Type type,
                    const String& keyspace_name, const String& target_name = "",
                    const Vector<String>& args_types = Vector<String>())
    : Event(encode(target, type, keyspace_name, target_name, args_types)) { }

  static String encode(Target target, Type type,
                       const String& keyspace_name, const String& target_name,
                       const Vector<String>& arg_types);
};

class Cluster {
protected:
  void init(AddressGenerator& generator,
            ClientConnectionFactory& factory,
            size_t num_nodes_dc1,
            size_t num_nodes_dc2);

public:
  ~Cluster();

  String use_ssl(const String& cn = "");

  int start_all(EventLoopGroup* event_loop_group);
  void start_all_async(EventLoopGroup* event_loop_group);

  void stop_all();
  void stop_all_async();

  int start(EventLoopGroup* event_loop_group, size_t node);
  void start_async(EventLoopGroup* event_loop_group, size_t node);

  void stop(size_t node);
  void stop_async(size_t node);

  int add(cass::EventLoopGroup* event_loop_group, size_t node);
  void remove(size_t node);

  const Host& host(const Address& address) const;
  Hosts hosts() const;

  void event(const Event::Ptr& event);

private:
  struct Server {
    Server(const Host& host, const internal::ServerConnection::Ptr& connection)
      : host(host)
      , connection(connection)
      , is_removed(false) { }

    Server(const Server& server)
      : host(server.host)
      , connection(server.connection)
      , is_removed(server.is_removed.load()) { }

    Server& operator=(const Server& server) {
      host = server.host;
      connection = server.connection;
      is_removed.store(server.is_removed.load());
      return *this;
    }

    Host host;
    internal::ServerConnection::Ptr connection;
    cass::Atomic<bool> is_removed;
  };

  typedef Vector<Server> Servers;

private:
  Servers servers_;
};

class SimpleEventLoopGroup : public cass::RoundRobinEventLoopGroup {
public:
  SimpleEventLoopGroup(size_t num_threads = 1);
  ~SimpleEventLoopGroup();
};

class SimpleRequestHandlerBuilder : public RequestHandler::Builder {
public:
  SimpleRequestHandlerBuilder();
};

class AuthRequestHandlerBuilder : public SimpleRequestHandlerBuilder {
public:
  AuthRequestHandlerBuilder(const String& username = "cassandra",
                            const String& password = "cassandra");
};

class SimpleCluster : public Cluster {
public:
  SimpleCluster(const RequestHandler* request_handler,
                size_t num_nodes_dc1 = 1,
                size_t num_nodes_dc2 = 0)
    : factory_(request_handler, this)
    , event_loop_group_(1) {
    init(generator_, factory_, num_nodes_dc1, num_nodes_dc2);
  }

  ~SimpleCluster() {
    stop_all();
  }

  void use_close_immediately() {
    factory_.use_close_immediately();
  }

  int start_all() {
    return Cluster::start_all(&event_loop_group_);
  }

  int start(size_t node) {
    return Cluster::start(&event_loop_group_, node);
  }

  int add(size_t node) {
    return Cluster::add(&event_loop_group_, node);
  }

private:
  Ipv4AddressGenerator generator_;
  ClientConnectionFactory factory_;
  SimpleEventLoopGroup event_loop_group_;
};

class SimpleEchoServer {
public:
  SimpleEchoServer(const Address& address = Address("127.0.0.1", 8888))
   : event_loop_group_(1)
   , server_(Memory::allocate<internal::ServerConnection>(address, factory_)) { }

  ~SimpleEchoServer() {
    close();
  }

  void close() {
    server_->close();
    server_->wait_close();
  }

  String use_ssl(const String& cn = "") {
    String key(Ssl::generate_key());
    String cert(Ssl::generate_cert(key, cn));
    if (!server_->use_ssl(key, cert)) {
      return "";
    }
    return cert;
  }

  void use_close_immediately() {
    factory_.use_close_immediately();
  }

  int listen() {
    server_->listen(&event_loop_group_);
    return server_->wait_listen();
  }

  void reset(const Address& address) {
    server_.reset(Memory::allocate<internal::ServerConnection>(address, factory_));
  }

private:
  class CloseConnection : public internal::ClientConnection {
  public:
    CloseConnection(internal::ServerConnection* server)
      : internal::ClientConnection(server) { }

    virtual int on_accept() {
      int rc = accept();
      if (rc != 0) {
        return rc;
      }
      close();
      return rc;
    }
  };

  class EchoConnection : public internal::ClientConnection {
  public:
    EchoConnection(internal::ServerConnection* server)
      : internal::ClientConnection(server) { }

    virtual void on_read(const char* data, size_t len)  {
      write(data, len);
    }
  };

  class ClientConnectionFactory : public internal::ClientConnectionFactory {
  public:
    ClientConnectionFactory()
      : close_immediately_(false) { }

    void use_close_immediately() {
      close_immediately_ = true;
    }

    virtual internal::ClientConnection* create(internal::ServerConnection* server) const {
      if (close_immediately_) {
        return Memory::allocate<CloseConnection>(server);
      } else {
        return Memory::allocate<EchoConnection>(server);
      }
    }

  private:
    bool close_immediately_;
  };

private:
  ClientConnectionFactory factory_;
  SimpleEventLoopGroup event_loop_group_;
  internal::ServerConnection::Ptr server_;
};

} // namespace mockssandra

#endif // MOCKSSANDRA_HPP
