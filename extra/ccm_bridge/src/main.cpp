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

void setup_boost_log() {
	boost::log::add_common_attributes();
	/*boost::log::add_file_log
    (
        keywords::file_name = "log.txt",                                        
        keywords::format = "[%TimeStamp%] (%Severity%): %Message%"                                 
    );
	*/

 //	  BOOST_LOG_TRIVIAL(trace) << "A trace severity message";
 //   BOOST_LOG_TRIVIAL(debug) << "A debug severity message";
 //   BOOST_LOG_TRIVIAL(info) << "An informational severity message";
 //   BOOST_LOG_TRIVIAL(warning) << "A warning severity message";
 //   BOOST_LOG_TRIVIAL(error) << "An error severity message";
 //   BOOST_LOG_TRIVIAL(fatal) << "A fatal severity message";
}

int main() { 
	setup_boost_log();

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