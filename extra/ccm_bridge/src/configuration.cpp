#include <fstream>
#include <boost/algorithm/string.hpp>
#include <boost/log/trivial.hpp>
#include <boost/lexical_cast.hpp>

#include "configuration.hpp"

using namespace std;

namespace CCMBridge {
	const int Configuration::DEFAULT_SSH_PORT = 22;

	Configuration::Configuration() 
		: _ssh_port(22),
		  _ssh_host("localhost"),
		  _ssh_user(), 
		  _ssh_pass(),
		  _cassandra_version("1.2.5"),
		  _use_compression(false),
		  _use_buffering(true),
		  _use_logger(false),
		  _ip_prefix()
	{
	}

	const string& Configuration::ip_prefix() const {
		return _ip_prefix;
	}

	const string& Configuration::cassandara_version() const {
		return _cassandra_version;
	}

	const string& Configuration::ssh_host() const {
		return _ssh_host;
	}

	int Configuration::ssh_port() const{
		return _ssh_port;
	}

	const string& Configuration::ssh_username() const {
		return _ssh_user;
	}

	const string& Configuration::ssh_password() const {
		return _ssh_pass;
	}

	bool Configuration::use_buffering() const {
		return _use_buffering;
	}

	bool Configuration::use_logger() const {
		return _use_logger;
	}

	bool Configuration::use_compression() const {
		return _use_compression;
	}

	Configuration::KeyValuePairs Configuration::get_settings(const string& file_name) {
		KeyValuePairs settings;

		string line;
		ifstream settings_file(file_name, ios_base::in);

		if(!settings_file) {
			BOOST_LOG_TRIVIAL(error) 
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

	bool Configuration::is_empty(string line) {
		boost::trim(line);
		return line.empty();
	}

	bool Configuration::is_comment(string line) {
		boost::trim(line);
		return boost::starts_with(line, "#");
	}

	void Configuration::add_setting(KeyValuePairs& settings, string line) {
		boost::trim(line);

		size_t eq_pos = line.find('=');
		if(eq_pos != string::npos) {
			string key = line.substr(0, eq_pos);
			string value = line.substr(eq_pos+1, line.size());

			boost::trim(key); boost::to_lower(key);
			boost::trim(value);

			if(!key.empty() && !value.empty()) {
				settings[key] = value;
				BOOST_LOG_TRIVIAL(info) 
					<< "Configuration key: " << key
					<< " equals value: " << value;
				return;
			}
		}

		BOOST_LOG_TRIVIAL(warning)
			<< "Invalid configuration entry: " 
			<< line;
	}

	void Configuration::apply_settings(const KeyValuePairs& settings) {
		for(KeyValuePairs::const_iterator it = settings.begin();
			it != settings.end(); ++it) 
		{
			apply_setting(/* key: */ it->first, /* value: */ it->second);
		}
	}

	bool Configuration::to_bool(const string& value) {
		return (value == "yes"  ||
				value == "YES"  ||
				value == "TRUE" ||
				value == "True" ||
				value == "true" ||
				value == "1");
	}

	void Configuration::apply_setting(const string& key, const string& value) {
		if(key == "ssh_username") {
			_ssh_user = value;
		}
		else if(key == "ssh_password") {
			_ssh_pass = value;
		}
		else if(key == "ssh_port") {
			try {
				_ssh_port = boost::lexical_cast<int>(value);
			}
			catch(boost::bad_lexical_cast&) {
				BOOST_LOG_TRIVIAL(error)
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
			BOOST_LOG_TRIVIAL(warning)
				<< "Unknown configuration option: "
				<< key << " with value " << value;
		}
	}

	void Configuration::read_configuration(const std::string& file_name) {
		KeyValuePairs settings = get_settings(file_name);
		apply_settings(settings);
	}

	// Singleton implementation by static variable
	const Configuration& get_configuration() {
		static Configuration config;
		bool initialized = false;

		if(!initialized) {
			const std::string CONFIG_FILE_NAME = "config.txt";
			config.read_configuration(CONFIG_FILE_NAME);

			initialized = true;
		}

		return config;
	}
}
