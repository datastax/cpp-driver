#include <boost/shared_ptr.hpp>

namespace cql
{
	class cql_ccm_bridge_t;
	class cql_builder_t;
}
	
void ContinueTheConsistencyTest(
	boost::shared_ptr<cql::cql_ccm_bridge_t> ccm,
	boost::shared_ptr<cql::cql_builder_t> builder );
