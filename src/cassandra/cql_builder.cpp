#include "cassandra/cql_cluster.hpp"
#include "cassandra/cql_builder.hpp"

namespace cql {

boost::shared_ptr<cql_cluster_t> cql_builder_t::build() {
    return cql_cluster_t::built_from(*this);
}

}