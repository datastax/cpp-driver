#ifndef _CASSANDRA_CCM_BRIDGE_H_INCLUDE_
#define _CASSANDRA_CCM_BRIDGE_H_INCLUDE_

#include <exception>
#include <libssh2.h>
#include "configuration.hpp"

namespace CCMBridge {
	class CCMBridge {
	public:
		CCMBridge(const Configuration& settings);
		~CCMBridge();


	private:
		void initialize_socket_library();
		void finalize_socket_library();

		void start_connection(const Configuration& settings);
		void close_socket();
		void start_ssh_connection(const Configuration& settings);
		void close_ssh_session();

		int socket;
		LIBSSH2_SESSION* session;
		LIBSSH2_CHANNEL* channel;
	};

	class CCMBridgeException : public std::exception { 
	public:
		CCMBridgeException(const char* message)
			: _message(message) { }

		virtual const char* what() { 
			return _message;
		}
	private:
		const char* const _message;
	};
}

#endif // _CASSANDRA_CCM_BRIDGE_H_INCLUDE_