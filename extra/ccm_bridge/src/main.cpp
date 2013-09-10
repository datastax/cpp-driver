#include <iostream>
#include <vector>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <string>

#include "configuration.hpp"
#include "ccm_bridge.hpp"

using namespace std;
using namespace Cassandra;

namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

int main() { 
	//boost::log::add_file_log("log.txt");
	try  {
		const Configuration& config = get_configuration();
		CCMBridge b(config);

		string line;
		while(true) {
			getline(cin, line);
			string result = b.execute_command(line);
			std::cout << result;
		}

	}
	catch(CCMBridgeException& e) {
		std::cerr << "error: " << e.what() << std::endl;
	}


	system("pause");
}