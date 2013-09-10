
#include <algorithm>
#include <iterator>
#include <boost/thread.hpp>
#include <boost/log/trivial.hpp>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>

#include "ccm_bridge.hpp"
#include "safe_advance.hpp"

using namespace std;

const int SSH_STDOUT = 0;
const int SSH_STDERR = 1;

namespace Cassandra {
	const string CCMBridge::CCM_COMMAND = "ccm";

	CCMBridge::CCMBridge(const Configuration& settings) 
		:	_socket(-1),
			_session(0), _channel(0),
			_ip_prefix(settings.ip_prefix())
	{
		initialize_socket_library();

		try { 
			// initialize libssh2 - not thread safe
			if(0 != libssh2_init(0))
				throw CCMBridgeException("cannot initialized libssh2 library");

			try {
				start_connection(settings);

				try {
					start_ssh_connection(settings);
				}
				catch(CCMBridgeException&) {
					close_socket();
					throw;
				}
			}
			catch(CCMBridgeException&) {
				libssh2_exit();
				throw;
			}
		}
		catch(CCMBridgeException&) {
			finalize_socket_library();
			throw;
		}

		initialize_environment();
	}

	CCMBridge::~CCMBridge() {
		libssh2_channel_free(_channel);
		close_ssh_session();
		
		close_socket();
		libssh2_exit();
		finalize_socket_library();
	}

	void CCMBridge::start_connection(const Configuration& settings) {
		_socket = ::socket(AF_INET, SOCK_STREAM, 0);
		if(_socket == -1)
			throw CCMBridgeException("cannot create socket");

		sockaddr_in socket_address;

		socket_address.sin_family = AF_INET;
		socket_address.sin_port = htons(settings.ssh_port());
		socket_address.sin_addr.s_addr = inet_addr(settings.ssh_host().c_str());

		int result = connect(_socket, 
							 reinterpret_cast<sockaddr *>(&socket_address),
							 sizeof(socket_address));
		if(result == -1) {
			close_socket();
			throw new CCMBridgeException("cannot connect to remote host");
		}
	}

	void CCMBridge::close_ssh_session() {
		libssh2_session_disconnect(_session, "Requested by user.");
		libssh2_session_free(_session);
	}

	void CCMBridge::start_ssh_connection(const Configuration& settings) {
		 _session = libssh2_session_init();
		 if(!_session)
			 throw new CCMBridgeException("cannot create ssh session");

		 try {
			if (libssh2_session_handshake(_session, _socket))
				throw CCMBridgeException("ssh session handshake failed");

			// get authentication modes supported by server
			char* auth_methods = libssh2_userauth_list(
									_session,
									settings.ssh_username().c_str(), 
									settings.ssh_username().size());

			if(strstr(auth_methods, "password") == NULL)
				throw CCMBridgeException("server doesn't support authentication by password");

			// try to login using username and password
			int auth_result = libssh2_userauth_password(_session, 
										  settings.ssh_username().c_str(), 
										  settings.ssh_password().c_str());
			
			if(auth_result != 0)
				throw CCMBridgeException("invalid password or user");

			if (!(_channel = libssh2_channel_open_session(_session)))
				throw CCMBridgeException("cannot open ssh session");

			try {

				if (libssh2_channel_request_pty(_channel, "vanilla"))
					throw CCMBridgeException("pty requests failed");

				if (libssh2_channel_shell(_channel))
					throw CCMBridgeException("cannot open shell");
			}
			catch(CCMBridgeException&) {
				// calls channel_close
				libssh2_channel_free(_channel);
			}
		}
		catch(CCMBridgeException&) {
			close_ssh_session();
			throw;
		}
	}

	void CCMBridge::close_socket() {
#ifdef WIN32
		closesocket(_socket);
#else
		close(socket);
#endif
		_socket = -1;
	}

	void CCMBridge::initialize_socket_library() {
#ifdef WIN32
		WSADATA wsadata;
		if(0 != WSAStartup(MAKEWORD(2,0), &wsadata)) {
			throw new CCMBridgeException("cannot initialize windows sockets");
		}
#endif
	}

	void CCMBridge::finalize_socket_library() {
#ifdef WIN32
		WSACleanup();
#endif
	}

	void CCMBridge::initialize_environment() {
		wait_for_shell_prompth();

		// clear buffors
		_esc_remover_stdout.clear_buffer();
		_esc_remover_stdout.clear_buffer();

		// disable terminal echo 
		execute_command("stty -echo");
	}

	string CCMBridge::execute_command(const string& command) {
		terminal_write(command);
		terminal_write("\n");

		wait_for_shell_prompth();

		string result = "";

		result += terminal_read_stdout();
		result += terminal_read_stderr();

		return result;
	}

	void CCMBridge::wait_for_shell_prompth() {
		const char SHELL_PROMPTH_CHARACTER = '$';
		
		while(!_esc_remover_stdout.ends_with_character(SHELL_PROMPTH_CHARACTER)) {
			if(libssh2_channel_eof(_channel))
				throw CCMBridgeException("connection closed by remote host");

			terminal_read_stream(_esc_remover_stdout, SSH_STDOUT);
			boost::this_thread::sleep(boost::posix_time::milliseconds(50));
		}
	}

	string CCMBridge::terminal_read_stdout() {
		return terminal_read(_esc_remover_stdout, SSH_STDOUT);
	}

	string CCMBridge::terminal_read_stderr() {
		return terminal_read(_esc_remover_stderr, SSH_STDERR);
	}

	string CCMBridge::terminal_read(EscapeSequencesRemover& buffer, int stream) {
		terminal_read_stream(buffer, stream);

		if(buffer.data_available())
			return buffer.get_buffer_contents();

		return string();
	}

	void CCMBridge::terminal_read_stream(EscapeSequencesRemover& buffer, int stream) {
		char buf[128];
		ssize_t readed;

		while(true) {
			// disable blocking
			libssh2_session_set_blocking(_session, 0);
			
			readed = libssh2_channel_read_ex(_channel, stream, buf, sizeof(buf));
			
			// return if no data to read
			if(readed == LIBSSH2_ERROR_EAGAIN || readed == 0)
				return;

			// some error occurred
			if(readed < 0)
				throw CCMBridgeException("error during reading from socket");

			buffer.push_character_range(buf, buf+readed);
		}
	}

	void CCMBridge::terminal_write(const string& command) {
		// enable blocking
		libssh2_channel_set_blocking(_channel, 1);
		libssh2_channel_write(_channel, command.c_str(), command.size());
	}

	void CCMBridge::execute_ccm_command(const string& ccm_args, 
										bool use_already_existing)
	{
		const int RETRY_TIMES = 2;

		for(int retry = 0; retry < RETRY_TIMES; retry++) {
			BOOST_LOG_TRIVIAL(info) << "CCM " << ccm_args;
			string result = execute_command(CCM_COMMAND + " " + ccm_args);
			
			if(boost::algorithm::contains(result, "[Errno")) {
				BOOST_LOG_TRIVIAL(error) << "CCM ERROR: " << result;
			}

			if(boost::algorithm::contains(result, "[Errno 17")) {
				if (use_already_existing)
					return;

				execute_ccm_and_print("remove test");
				execute_command("killall java");

				throw CCMBridgeException("not implemented ReusableCCMCluster.Reset();");
			}
		}
		
		throw CCMBridgeException("ccm operation failed");
	}

	void CCMBridge::execute_ccm_and_print(const string& ccm_args) {
		BOOST_LOG_TRIVIAL(info) << "CCM " << ccm_args;
		string result = execute_command(CCM_COMMAND + " " + ccm_args);
			
		if(boost::algorithm::contains(result, "[Errno")) {
			BOOST_LOG_TRIVIAL(error) << "CCM ERROR: " << result;
		}
		else {
			BOOST_LOG_TRIVIAL(info) << "CCM RESULT: " << result;
		}
	}

	void CCMBridge::start() {
		execute_ccm_command("start");
	}

	void CCMBridge::start(int node) {
		execute_ccm_command(str(boost::format("node%1% start") % node));
	}

	void CCMBridge::stop() {
		execute_ccm_command("stop");
	}

	void CCMBridge::stop(int node) {
		execute_ccm_command(str(boost::format("node%1% stop") % node));
	}

	void CCMBridge::kill() {
		execute_ccm_command("stop --not-gently");
	}

	void CCMBridge::kill(int node) {
		execute_ccm_command(str(boost::format("node%1% stop --not-gently") % node));
	}

	void CCMBridge::remove() {
		stop();
		execute_ccm_command("remove");
	}

	void CCMBridge::ring(int node) {
		execute_ccm_command(str(boost::format("node%1% ring") % node));
	}

	void CCMBridge::bootstrap(int node, const std::string& dc) {

	   if (dc.empty()) {
		   execute_ccm_command(str(
			   boost::format("add node%1% -i %2%%3% -j %4% -b") 
					% node
					% _ip_prefix
					% node
					% (7000 + 100 * node)));
	   }
       else {
		   execute_ccm_command(str(
			   boost::format("add node%1% -i %2%%3% -j %4% -b -d %5%") 
					% node
					% _ip_prefix
					% node
					% (7000 + 100 * node)
					% dc));

	   }

	   start(node);
	}

	void CCMBridge::decommission(int node) {
		execute_ccm_command(str(boost::format("node%1% decommission") % node));
	}
}