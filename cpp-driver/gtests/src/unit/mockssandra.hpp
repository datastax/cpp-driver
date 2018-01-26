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
using cass::Timer;
using cass::Vector;
using cass::RefCounted;
using cass::SharedRefPtr;
using cass::ScopedPtr;

namespace mockssandra {

class Ssl {
public:
  static String generate_key();
  static String gererate_cert(const String& key, const String cn = "localhost");
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

class ServerConnection : public RefCounted<ServerConnection> {
public:
  typedef SharedRefPtr<ServerConnection> Ptr;

   ServerConnection(const ClientConnectionFactory& factory);
   ~ServerConnection();

   uv_loop_t* loop() { return event_loop_->loop(); }
   SSL_CTX* ssl_context() { return ssl_context_; }

   bool use_ssl(const String& key,
                const String& cert,
                const String& password = "");

   void listen(EventLoopGroup* event_loop_group, const Address& address);
   int wait_listen();

   void close();
   void wait_close();

private:
   friend class ClientConnection;
   int accept(uv_stream_t* client);
   void remove(ClientConnection* connection);

private:
   friend class RunListen;
   friend class RunClose;

   void internal_listen(const Address& address);
   void internal_close();
   void maybe_close();

   void signal_listen(int rc);
   void signal_close();

   static void on_connection(uv_stream_t* stream, int status);
   void handle_connection(int status);

   static void on_async(uv_async_t* async);
   void handle_async();

   static void on_close(uv_handle_t* handle);
   void handle_close();

   static int on_password(char *buf, int size, int rwflag, void *password);

private:
   typedef Vector<ClientConnection*> Vec;

   enum State {
     STATE_CLOSED,
     STATE_CLOSING,
     STATE_ERROR,
     STATE_PENDING,
     STATE_LISTENING,
   };

   Tcp tcp_;
   EventLoop* event_loop_;
   State state_;
   int rc_;
   uv_mutex_t mutex_;
   uv_cond_t cond_;
   Vec connections_;
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
  QUERY_FLAG_KEYSPACE = 0x80,
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

class ClientConnection;
class Request;

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

    Builder& void_result();
    Builder& no_result();

    Builder& use_keyspace(const String& keyspace);
    Builder& plaintext_auth(const String& username,
                            const String& password);

    Builder& validate_startup();
    Builder& validate_credentials();
    Builder& validate_auth_response();
    Builder& validate_register();
    Builder& validate_query();

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

  int8_t opcode() const { return opcode_; }

  void write(int8_t opcode, const String& body);
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

private:
  String encode_header(int8_t opcode_, int32_t len);

  static void on_timeout(Timer* timer);

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

struct VoidResult : public Action {
  virtual bool on_run(Request* request) const;
};

struct NoResult : public Action {
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

struct MatchQuery : public Action {
  virtual bool on_run(Request* request) const;
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

struct ValidateRegister: public Action {
  virtual bool on_run(Request* request) const;
};

struct ValidateQuery : public Action {
  virtual bool on_run(Request* request) const;
};

class RequestHandler {
public:
  class Builder {
  public:
    Builder() {
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

  private:
    Action::Builder actions_[OPCODE_LAST_ENTRY];
    Action::Builder invalid_protocol_;
    Action::Builder invalid_opcode_;
    Action::Builder dummy_;
  };

  RequestHandler(Builder* builder);

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
  ClientConnection(internal::ServerConnection* server, const RequestHandler* request_handler)
    : internal::ClientConnection(server)
    , handler_(request_handler) { }
  ~ClientConnection();

  virtual void on_read(const char* data, size_t len);

  void add(Request* request) { requests_.add_to_back(request); }
  void remove(Request* request) { requests_.remove(request); }

private:
  ProtocolHandler handler_;
  List<Request> requests_;
};

class CloseConnection : public ClientConnection {
public:
  CloseConnection(internal::ServerConnection* server, const RequestHandler* request_handler)
    : ClientConnection(server, request_handler) { }

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
  ClientConnectionFactory(const RequestHandler* request_handler)
    : request_handler_(request_handler)
    , close_immediately_(false) { }

  void use_close_immediately() {
    close_immediately_ = true;
  }

  virtual internal::ClientConnection* create(internal::ServerConnection* server) const {
    if (close_immediately_) {
      return Memory::allocate<CloseConnection>(server, request_handler_.get());
    } else {
      return Memory::allocate<ClientConnection>(server, request_handler_.get());
    }
  }

private:
  ScopedPtr<const RequestHandler> request_handler_;
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

class Cluster {
protected:
  void init(AddressGenerator& generator,
            ClientConnectionFactory& factory,
            size_t num_nodes);

public:
  ~Cluster();

  String use_ssl();

  int start_all(EventLoopGroup* event_loop_group);
  void start_all_async(EventLoopGroup* event_loop_group);

  void stop_all();
  void stop_all_async();

  int start(EventLoopGroup* event_loop_group, size_t node);
  void start_async(EventLoopGroup* event_loop_group, size_t node);

  void stop(size_t node);
  void stop_async(size_t node);

private:
  struct Server {
    Server(const Address& address, const internal::ServerConnection::Ptr& connection)
      : address(address)
      , connection(connection) { }
    Address address;
    internal::ServerConnection::Ptr connection;
  };
  typedef Vector<Server> Servers;

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

class SimpleCluster : public Cluster {
public:
  SimpleCluster(const RequestHandler* request_handler,
                size_t num_nodes = 1)
    : factory_(request_handler)
    , event_loop_group_(1) {
    init(generator_, factory_, num_nodes);
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

private:
  Ipv4AddressGenerator generator_;
  ClientConnectionFactory factory_;
  SimpleEventLoopGroup event_loop_group_;
};

class SimpleEchoServer {
public:
  SimpleEchoServer()
   : event_loop_group_(1)
   , server_(Memory::allocate<internal::ServerConnection>(factory_)) { }

  ~SimpleEchoServer() {
    close();
  }

  void close() {
    server_->close();
  }

  String use_ssl() {
    String key(Ssl::generate_key());
    String cert(Ssl::gererate_cert(key));
    if (!server_->use_ssl(key, cert)) {
      return "";
    }
    return cert;
  }

  void use_close_immediately() {
    factory_.use_close_immediately();
  }

  int listen(const Address& address = Address("127.0.0.1", 8888)) {
    server_->listen(&event_loop_group_, address);
    return server_->wait_listen();
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
