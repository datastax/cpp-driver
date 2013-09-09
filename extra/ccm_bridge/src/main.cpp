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
#include "ccm_bridge.hpp"

using namespace std;
using namespace CCMBridge;

namespace src = boost::log::sources;
namespace sinks = boost::log::sinks;
namespace keywords = boost::log::keywords;

int main() { 
	//boost::log::add_file_log("log.txt");
	{
	const Configuration& config = get_configuration();

	::CCMBridge::CCMBridge b(config);
	}

	system("pause >nul");
}