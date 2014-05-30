#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include "logger.hpp"
#include "cql_ccm_bridge_configuration.hpp"

using namespace std;

namespace cql {
	const int cql_ccm_bridge_configuration_t::DEFAULT_SSH_PORT = 22;

	cql_ccm_bridge_configuration_t::cql_ccm_bridge_configuration_t() 
    :     _ip_prefix(),
          _cassandra_version("1.2.5"),
          _ssh_host("localhost"),
          _ssh_port(22),
          _ssh_user(),
          _ssh_pass(),
          _use_buffering(true),
          _use_logger(false),
          _use_compression(false)
	{
	}

	const string& cql_ccm_bridge_configuration_t::ip_prefix() const {
		return _ip_prefix;
	}

	const string& cql_ccm_bridge_configuration_t::cassandara_version() const {
		return _cassandra_version;
	}

	const string& cql_ccm_bridge_configuration_t::ssh_host() const {
		return _ssh_host;
	}

	short cql_ccm_bridge_configuration_t::ssh_port() const{
		return _ssh_port;
	}

	const string& cql_ccm_bridge_configuration_t::ssh_username() const {
		return _ssh_user;
	}

	const string& cql_ccm_bridge_configuration_t::ssh_password() const {
		return _ssh_pass;
	}

	bool cql_ccm_bridge_configuration_t::use_buffering() const {
		return _use_buffering;
	}

	bool cql_ccm_bridge_configuration_t::use_logger() const {
		return _use_logger;
	}

	bool cql_ccm_bridge_configuration_t::use_compression() const {
		return _use_compression;
	}

	cql_ccm_bridge_configuration_t::settings_t cql_ccm_bridge_configuration_t::get_settings(const string& file_name) {
		settings_t settings;

		string line;
		ifstream settings_file(file_name.c_str(), ios_base::in);

		if(!settings_file) {
			CQL_LOG(error) 
				<< "Cannot open configuration file: " << file_name;
			return settings;
		}

		while(getline(settings_file, line)) {
			if(is_comment(line) || is_empty(line))
				continue;

			add_setting(settings, line);
		}

		return settings;
	}

	bool cql_ccm_bridge_configuration_t::is_empty(string line) {
		boost::trim(line);
		return line.empty();
	}

	bool cql_ccm_bridge_configuration_t::is_comment(string line) {
		boost::trim(line);
		return boost::starts_with(line, "#");
	}

	void cql_ccm_bridge_configuration_t::add_setting(settings_t& settings, string line) {
		boost::trim(line);

		size_t eq_pos = line.find('=');
		if(eq_pos != string::npos) {
			string key = line.substr(0, eq_pos);
			string value = line.substr(eq_pos+1, line.size());

			boost::trim(key); boost::to_lower(key);
			boost::trim(value);

			if(!key.empty() && !value.empty()) {
				settings[key] = value;
				CQL_LOG(info)
					<< "Configuration key: " << key
					<< " equals value: " << value;
				return;
			}
		}

		CQL_LOG(warning)
			<< "Invalid configuration entry: " 
			<< line;
	}

	void cql_ccm_bridge_configuration_t::apply_settings(const settings_t& settings) {
		for(settings_t::const_iterator it = settings.begin();
			it != settings.end(); ++it) 
		{
			apply_setting(/* key: */ it->first, /* value: */ it->second);
		}
	}

	bool cql_ccm_bridge_configuration_t::to_bool(const string& value) {
		return (value == "yes"  ||
				value == "YES"  ||
				value == "TRUE" ||
				value == "True" ||
				value == "true" ||
				value == "1");
	}

	void cql_ccm_bridge_configuration_t::apply_setting(const string& key, const string& value) {
		if(key == "ssh_username") {
			_ssh_user = value;
		}
		else if(key == "ssh_password") {
			_ssh_pass = value;
		}
		else if(key == "ssh_port") {
			try {
				_ssh_port = boost::lexical_cast<short>(value);
			}
			catch(boost::bad_lexical_cast&) {
				CQL_LOG(error)
					<< "Invalid SSH_PORT value: " << value;
				_ssh_port = DEFAULT_SSH_PORT;
			}
		}
		else if(key == "ssh_host") {
			_ssh_host = value;
		}
		else if(key == "ip_prefix") {
			_ip_prefix = value;
		}
		else if(key == "cassandra_version") {
			_cassandra_version = value;
		}
		else if(key == "use_compression") {
			_use_compression = to_bool(value);
		}
		else if(key == "use_buffering") {
			_use_buffering = to_bool(value);
		}
		else if(key == "use_logger") {
			_use_logger = to_bool(value);
		}
		else {
			CQL_LOG(warning)
				<< "Unknown configuration option: "
				<< key << " with value " << value;
		}
	}

	void cql_ccm_bridge_configuration_t::read_configuration(const std::string& file_name) {
		settings_t settings = get_settings(file_name);
		apply_settings(settings);
	}

	// Singleton implementation by static variable
	const cql_ccm_bridge_configuration_t& get_ccm_bridge_configuration() {
		static cql_ccm_bridge_configuration_t config;
		static bool initialized = false;

		if(!initialized) {
			const std::string CONFIG_FILE_NAME = "config.txt";
			config.read_configuration(CONFIG_FILE_NAME);

			initialized = true;
		}

		return config;
	}
}
