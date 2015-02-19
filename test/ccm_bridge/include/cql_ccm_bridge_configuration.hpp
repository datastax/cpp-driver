/*
  Copyright (c) 2014-2015 DataStax

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

#ifndef CQL_CCM_BRIDGE_CONFIGURATION_H_
#define CQL_CCM_BRIDGE_CONFIGURATION_H_

#include <string>
#include <exception>
#include <map>

#include <boost/noncopyable.hpp>

namespace cql {
  class cql_ccm_bridge_configuration_t: public boost::noncopyable {
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

		cql_ccm_bridge_configuration_t();

		void read_configuration(const std::string& file_name);		 
		
		bool is_comment(std::string line);
		bool is_empty(std::string line);

		typedef
			std::map<std::string, std::string> 
			settings_t;

		settings_t get_settings(const std::string& file_name);
		void add_setting(settings_t& settings, std::string line);

		void apply_settings(const settings_t& settings);
		void apply_setting(const std::string& key, const std::string& value);
		bool to_bool(const std::string& value);

		friend const cql_ccm_bridge_configuration_t& get_configuration();

		std::string _ip_prefix;
		std::string _cassandra_version;
		std::string _ssh_host;
		short _ssh_port;
		std::string _ssh_user;
		std::string _ssh_pass;
		bool _use_buffering;
		bool _use_logger;
		bool _use_compression;
        
    friend const cql_ccm_bridge_configuration_t& get_ccm_bridge_configuration(const std::string&);
	};



	// Returns current tests configuration.
	// Configuration is read from config.txt file.
  const cql_ccm_bridge_configuration_t& get_ccm_bridge_configuration(const std::string& filename = "config.txt");
}

#endif // CQL_CCM_BRIDGE_CONFIGURATION_H_
