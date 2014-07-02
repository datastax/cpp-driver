#include "cassandra.h"

extern "C" {

void cass_paging_state_free(const CassPagingState* paging_state) {
  delete paging_state->from();
}

} // extern "C"
