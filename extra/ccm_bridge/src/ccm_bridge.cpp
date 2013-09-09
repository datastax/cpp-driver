
#include "ccm_bridge.hpp"

using namespace std;

namespace CCMBridge {
	CCMBridge::CCMBridge(const Configuration& settings) {
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
	}

	CCMBridge::~CCMBridge() {
		libssh2_channel_free(channel);
		close_ssh_session();
		
		close_socket();
		libssh2_exit();
		finalize_socket_library();
	}

	void CCMBridge::start_connection(const Configuration& settings) {
		socket = ::socket(AF_INET, SOCK_STREAM, 0);
		if(socket == -1)
			throw CCMBridgeException("cannot create socket");

		sockaddr_in socket_address;

		socket_address.sin_family = AF_INET;
		socket_address.sin_port = htons(settings.ssh_port());
		socket_address.sin_addr.s_addr = inet_addr(settings.ssh_host().c_str());

		int result = connect(socket, 
							 reinterpret_cast<sockaddr *>(&socket_address),
							 sizeof(socket_address));
		if(result == -1) {
			close_socket();
			throw new CCMBridgeException("cannot connect to remote host");
		}
	}

	void CCMBridge::close_ssh_session() {
		libssh2_session_disconnect(session, "Requested by user.");
		libssh2_session_free(session);
	}

	void CCMBridge::start_ssh_connection(const Configuration& settings) {
		 session = libssh2_session_init();
		 if(!session)
			 throw new CCMBridgeException("cannot create ssh session");

		 try {
			if (libssh2_session_handshake(session, socket))
				throw CCMBridgeException("ssh session handshake failed");

			// get authentication modes supported by server
			char* auth_methods = libssh2_userauth_list(
									session,
									settings.ssh_username().c_str(), 
									settings.ssh_username().size());

			if(strstr(auth_methods, "password") == NULL)
				throw CCMBridgeException("server doesn't support authentication by password");

			// try to login using username and password
			int auth_result = libssh2_userauth_password(session, 
										  settings.ssh_username().c_str(), 
										  settings.ssh_password().c_str());
			
			if(auth_result != 0)
				throw CCMBridgeException("invalid password or user");

			if (!(channel = libssh2_channel_open_session(session)))
				throw CCMBridgeException("cannot open ssh session");

			try {

				if (libssh2_channel_request_pty(channel, "vt100"))
					throw CCMBridgeException("pty requests failed");

				if (libssh2_channel_shell(channel))
					throw CCMBridgeException("cannot open shell");
			}
			catch(CCMBridgeException&) {
				// calls channel_close
				libssh2_channel_free(channel);
			}
		}
		catch(CCMBridgeException&) {
			close_ssh_session();
			throw;
		}
	}

	void CCMBridge::close_socket() {
#ifdef WIN32
		closesocket(socket);
#else
		close(socket);
#endif
		socket = -1;
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
}