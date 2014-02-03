#ifndef CQL_CCM_BRIDGE_H_
#define CQL_CCM_BRIDGE_H_

#include <exception>
#include <deque>

#include <boost/smart_ptr.hpp>
#include <boost/noncopyable.hpp>

#include "cql_ccm_bridge_configuration.hpp"
#include "cql_escape_sequences_remover.hpp"

namespace cql {
	class cql_ccm_bridge_t: public boost::noncopyable {
	public:
		cql_ccm_bridge_t(const cql_ccm_bridge_configuration_t& settings);
		~cql_ccm_bridge_t();

		// Executes command on remote host
		// Returns command stdout and stderr followed by
		// shell prompth.
		std::string execute_command(const std::string& command);

		void start();
		void start(int node);
		void stop();
		void stop(int node);
		void kill();
		void kill(int node);

		void remove();
		void ring(int node);

		void bootstrap(int node, const std::string& dc = "");

		void decommission(int node);

		static boost::shared_ptr<cql_ccm_bridge_t> create(
			const cql_ccm_bridge_configuration_t& settings,
			const std::string& name);

		static boost::shared_ptr<cql_ccm_bridge_t> create(
			const cql_ccm_bridge_configuration_t& settings,
			const std::string& name,
			unsigned nodes_count);

		static boost::shared_ptr<cql_ccm_bridge_t> create(
			const cql_ccm_bridge_configuration_t& settings,
			const std::string& name,
			unsigned nodes_count_dc1,
			unsigned nodes_count_dc2);
	private:
		/* CCM functionality */
		static const std::string CCM_COMMAND;
		const std::string _ip_prefix;

		void execute_ccm_command(const std::string& ccm_args);

		void execute_ccm_and_print(const std::string& ccm_args);

		/* SSH connection functionality */

		void initialize_environment();
		void wait_for_shell_prompth();

		std::string terminal_read_stdout();
		std::string terminal_read_stderr();
		std::string terminal_read(cql_escape_sequences_remover_t& buffer, int stream);
		void terminal_read_stream(cql_escape_sequences_remover_t& buffer, int stream);

		void terminal_write(const std::string& command);
		
		void initialize_socket_library();
		void finalize_socket_library();

		void start_connection(const cql_ccm_bridge_configuration_t& settings);
		void close_socket();
		void start_ssh_connection(const cql_ccm_bridge_configuration_t& settings);
		void close_ssh_session();

		cql_escape_sequences_remover_t _esc_remover_stdout;
		cql_escape_sequences_remover_t _esc_remover_stderr;

		int _socket;
		struct ssh_internals;
		boost::scoped_ptr<ssh_internals> _ssh_internals;
	};

	class cql_ccm_bridge_exception_t : public std::exception { 
	public:
		cql_ccm_bridge_exception_t(const char* message)
			: _message(message) { }

		virtual const char* what() const throw() { 
			return _message;
		}
	private:
		const char* const _message;
	};
}

#endif // CQL_CCM_BRIDGE_H_
