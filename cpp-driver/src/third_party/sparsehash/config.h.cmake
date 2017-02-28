#cmakedefine HASH_FUN_H  <@HASH_FUN_H@>

/* the namespace of the hash<> function */
#cmakedefine HASH_NAMESPACE  @HASH_NAMESPACE@

#cmakedefine HASH_NAME  @HASH_NAME@

/* Define to 1 if you have the <inttypes.h> header file. */
#cmakedefine HAVE_INTTYPES_H  1

/* Define to 1 if you have the <stdint.h> header file. */
#cmakedefine HAVE_STDINT_H  1

/* Define to 1 if you have the <sys/types.h> header file. */
#cmakedefine HAVE_SYS_TYPES_H  1

/* Define to 1 if the system has the type `long long'. */
#cmakedefine HAVE_LONG_LONG  1

/* Define to 1 if you have the `memcpy' function. */
#cmakedefine HAVE_MEMCPY  1

/* Define to 1 if the system has the type `uint16_t'. */
#cmakedefine HAVE_UINT16_T 1

/* Define to 1 if the system has the type `u_int16_t'. */
#cmakedefine HAVE_U_INT16_T 1

/* Define to 1 if the system has the type `__uint16'. */
#cmakedefine HAVE___UINT16  1

/* The system-provided hash function including the namespace. */
#define SPARSEHASH_HASH  HASH_NAMESPACE::HASH_NAME

/* The system-provided hash function, in namespace HASH_NAMESPACE. */
#define SPARSEHASH_HASH_NO_NAMESPACE  HASH_NAME

/* Namespace for Google classes */
#define GOOGLE_NAMESPACE  ::sparsehash

/* Stops putting the code inside the Google namespace */
#define _END_GOOGLE_NAMESPACE_  }

/* Puts following code inside the Google namespace */
#define _START_GOOGLE_NAMESPACE_   namespace sparsehash {
