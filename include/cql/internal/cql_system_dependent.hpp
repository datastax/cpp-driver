
#ifndef CQL_SYSTEM_DEPENDENT_H_
#define CQL_SYSTEM_DEPENDENT_H_

// snprintf 
#include <cstdio>

#ifdef _WIN32
#define snprintf _snprintf
#endif


#endif // CQL_SYSTEM_DEPENDENT_H_