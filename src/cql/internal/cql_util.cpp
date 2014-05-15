/*
 *      Copyright (C) 2013 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include <cassert>
#include <string>

#ifndef CQL_NO_SNAPPY
    #include <snappy.h>
#endif

#include "cql/internal/cql_util.hpp"
#include "cql/exceptions/cql_exception.hpp"
#include "cql/exceptions/cql_driver_internal_error.hpp"

char*
cql::safe_strncpy(char* dest, const char* src, const size_t count) {
	assert(count > 0);

	strncpy(dest, src, count - 1);
	// null terminate destination
	dest[count - 1] = '\0';

	return dest;
}

bool
cql::to_ipaddr(const std::string& str, boost::asio::ip::address& result)
{
    boost::system::error_code err;

    boost::asio::ip::address tmp =
        boost::asio::ip::address::from_string(str, err);

    if (err) {
        return false;
    }
    
    result = tmp;

    return true;
}

namespace {
    
#ifndef CQL_NO_SNAPPY
std::vector<cql::cql_byte_t>
snappy_compress(const std::vector<cql::cql_byte_t>& buffer)
{
    size_t input_length = buffer.size(),
           output_length;
    
    std::vector<cql::cql_byte_t> output(snappy::MaxCompressedLength(input_length));
    snappy::RawCompress(reinterpret_cast<const char*>(&buffer[0]), input_length,
                        reinterpret_cast<      char*>(&output[0]), &output_length);
    
    output.resize(output_length);
    return output;
}
#endif

#ifndef CQL_NO_SNAPPY
void
snappy_compress_inplace(std::vector<cql::cql_byte_t>& buffer)
{
    const std::vector<cql::cql_byte_t>& out = snappy_compress(buffer);
    buffer = out;
}
#endif

#ifndef CQL_NO_SNAPPY
std::vector<cql::cql_byte_t>
snappy_uncompress(const std::vector<cql::cql_byte_t>& buffer)
{
    // Snappy crashes on empty buffers
    if (buffer.empty()) return std::vector<cql::cql_byte_t>();
    
    size_t input_length = buffer.size(),
           output_length;
    
    std::vector<cql::cql_byte_t> output;
    
    bool parsing_succeeded1 = true,
         parsing_succeeded2 = true;
    
    parsing_succeeded1 =
        snappy::GetUncompressedLength(reinterpret_cast<const char*>(&buffer[0]), input_length,
                                      &output_length);
    
    if (parsing_succeeded1) {
        output.resize(output_length);
    
        parsing_succeeded2 =
            snappy::RawUncompress(reinterpret_cast<const char*>(&buffer[0]), input_length,
                                  reinterpret_cast<      char*>(&output[0]) );
    }
    
    if (!parsing_succeeded1 || !parsing_succeeded2) {
        throw cql::cql_exception("Uncompression error"); // TODO: something more specific?
    }
    
    return output;
}
#endif

#ifndef CQL_NO_SNAPPY
void
snappy_uncompress_inplace(std::vector<cql::cql_byte_t>& buffer)
{
    const std::vector<cql::cql_byte_t>& out = snappy_uncompress(buffer);
    buffer = out;
}
#endif

#ifndef CQL_NO_SNAPPY
bool
is_valid_snappy_compressed_buffer(const std::vector<cql::cql_byte_t>& buffer)
{
    return snappy::IsValidCompressedBuffer(reinterpret_cast<const char*>(&buffer[0]),
                                           buffer.size());
}
#endif

} // End of anonymous namespace

//---------------------------------------------------------------------------------------------
std::vector<cql::cql_byte_t>
cql::compress(const std::vector<cql::cql_byte_t>& buffer, cql_compression_enum e)
{
    switch(e) {
        case CQL_COMPRESSION_NONE:
            // Do nothing.
            return buffer;
        case CQL_COMPRESSION_SNAPPY:
            #ifdef CQL_NO_SNAPPY
                throw cql_driver_internal_error_exception(
                    "Snappy compression requested, but unavailable. Recompile with snappy support.");
            #else
                return snappy_compress(buffer);
            #endif
        default:
            throw cql_driver_internal_error_exception("Requested an unknown compression algorithm.");
    }
}

void
cql::compress_inplace(std::vector<cql::cql_byte_t>& buffer, cql_compression_enum e)
{
    switch(e) {
        case CQL_COMPRESSION_NONE:
            // Do nothing.
            return;
        case CQL_COMPRESSION_SNAPPY:
            #ifdef CQL_NO_SNAPPY
                throw cql_driver_internal_error_exception(
                    "Snappy compression requested, but unavailable. Recompile with snappy support.");
            #else
                return snappy_compress_inplace(buffer);
            #endif
        default:
            throw cql_driver_internal_error_exception("Requested an unknown compression algorithm.");
    }
}

std::vector<cql::cql_byte_t>
cql::uncompress(const std::vector<cql::cql_byte_t>& buffer, cql_compression_enum e)
{
    switch(e) {
        case CQL_COMPRESSION_NONE:
            // Do nothing.
            return buffer;
        case CQL_COMPRESSION_SNAPPY:
            #ifdef CQL_NO_SNAPPY
                throw cql_driver_internal_error_exception(
                    "Snappy uncompression requested, but unavailable. Recompile with snappy support.");
            #else
                return snappy_uncompress(buffer);
            #endif
        default:
            throw cql_driver_internal_error_exception("Requested an unknown compression algorithm.");
    }
}

void
cql::uncompress_inplace(std::vector<cql::cql_byte_t>& buffer, cql_compression_enum e)
{
    switch(e) {
        case CQL_COMPRESSION_NONE:
            // Do nothing.
            return;
        case CQL_COMPRESSION_SNAPPY:
            #ifdef CQL_NO_SNAPPY
                throw cql_driver_internal_error_exception(
                    "Snappy uncompression requested, but unavailable. Recompile with snappy support.");
            #else
                return snappy_uncompress_inplace(buffer);
            #endif
        default:
            throw cql_driver_internal_error_exception("Requested an unknown compression algorithm.");
    }
}
//---------------------------------------------------------------------------------------------

boost::posix_time::ptime
cql::utc_now() {
    return boost::posix_time::microsec_clock::universal_time();
}
