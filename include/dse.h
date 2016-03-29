/*
  Copyright (c) 2014-2016 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __DSE_H_INCLUDED__
#define __DSE_H_INCLUDED__

#include <cassandra.h>

#if !defined(DSE_STATIC)
#  if (defined(WIN32) || defined(_WIN32))
#    if defined(DSE_BUILDING)
#      define DSE_EXPORT __declspec(dllexport)
#    else
#      define DSE_EXPORT __declspec(dllexport)
#    endif
#  elif (defined(__SUNPRO_C)  || defined(__SUNPRO_CC)) && !defined(DSE_STATIC)
#    define DSE_EXPORT __global
#  elif (defined(__GNUC__) && __GNUC__ >= 4) || defined(__INTEL_COMPILER)
#    define DSE_EXPORT __attribute__ ((visibility("default")))
#  endif
#else
#define DSE_EXPORT
#endif

#if defined(_MSC_VER)
#  define DSE_DEPRECATED(func) __declspec(deprecated) func
#elif defined(__GNUC__) || defined(__INTEL_COMPILER)
#  define DSE_DEPRECATED(func) func __attribute__((deprecated))
#else
#  define DSE_DEPRECATED(func) func
#endif

/**
 * @file include/dse.h
 *
 * C/C++ driver for DataStax Enterprise.
 */

#define DSE_VERSION_MAJOR 1
#define DSE_VERSION_MINOR 0
#define DSE_VERSION_PATCH 0
#define DSE_VERSION_SUFFIX "alpha"

#ifdef __cplusplus
extern "C" {
#endif

/**
 *
 */
typedef struct DseCluster_ DseCluster;

/**
 *
 */
DSE_EXPORT CassError
dse_cluster_set_gssapi_authentication(DseCluster* cluster,
                                      const char* service,
                                      const char* principle);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* __DSE_H_INCLUDED__ */
