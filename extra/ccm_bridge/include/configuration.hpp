#ifndef _CASSANDRA_CONFIGURATION_H_INCLUDE_
#define _CASSANDRA_CONFIGURATION_H_INCLUDE_

#include <string>
#include <exception>
#include <map>

namespace CCMBridge {
	class Configuration {
	public:
		const std::string& ip_prefix() const;
		const std::string& cassandara_version() const;

		const std::string& ssh_host() const;
		short ssh_port() const;
		const std::string& ssh_username() const;
		const std::string& ssh_password() const;

		bool use_buffering() const;
		bool use_logger() const;
		bool use_compression() const;
	
	private:
		const static int DEFAULT_SSH_PORT;

		Configuration();

		void read_configuration(const std::string& file_name);		 
		
		bool is_comment(std::string line);
		bool is_empty(std::string line);

		typedef
			std::map<std::string, std::string> 
			KeyValuePairs;

		KeyValuePairs get_settings(const std::string& file_name);
		void add_setting(KeyValuePairs& settings, std::string line);

		void apply_settings(const KeyValuePairs& settings);
		void apply_setting(const std::string& key, const std::string& value);
		bool to_bool(const std::string& value);

		friend const Configuration& get_configuration();

		std::string _ip_prefix;
		std::string _cassandra_version;
		std::string _ssh_host;
		short _ssh_port;
		std::string _ssh_user;
		std::string _ssh_pass;
		bool _use_buffering;
		bool _use_logger;
		bool _use_compression;
	};



	// Returns current tests configuration.
	// Configuration is readed from config.txt file.
	const Configuration& get_configuration();
}

#endif // _CASSANDRA_CONFIGURATION_H_INCLUDE_
