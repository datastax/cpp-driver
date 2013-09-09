#include <iostream>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>

#include "configuration.hpp"

using namespace std;
using namespace CCMBridge;

namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

int main() { 
	//boost::log::add_file_log("log.txt");

	const Configuration& config = get_configuration();

	std::cout << "ssh host: " << config.ssh_host() << std::endl;
	std::cout << "ssh port: " << config.ssh_port() << std::endl;
	std::cout << "ssh user: " << config.ssh_username() << std::endl;
	std::cout << "ssh pass: " << config.ssh_password() << std::endl;

	system("pause >nul");
}