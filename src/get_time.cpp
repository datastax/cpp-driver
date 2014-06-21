#include "get_time.hpp"

#if defined(WIN32) || defined(_WIN32)
#error Not implemented yet!
#elif defined(__APPLE__) && defined(__MACH__)
#include <mach/mach.h>
#include <mach/clock.h>
#else
#include <time.h>
#endif

namespace cass {

#if defined(WIN32) || defined(_WIN32)

uint64_t get_time_since_epoch() {
}

#elif defined(__APPLE__) && defined(__MACH__)

uint64_t get_time_since_epoch() {
  clock_serv_t clock_serv;
  mach_timespec_t ts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &clock_serv);
  clock_get_time(clock_serv, &ts);
  mach_port_deallocate(mach_task_self(), clock_serv);
  return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#else

uint64_t get_time_since_epoch() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#endif

}
