/*
  Copyright (c) 2014-2016 DataStax
*/

#include "external_types.hpp"
#include "line_string.hpp"
#include "validate.hpp"

extern "C" {

DseLineString* dse_line_string_new() {
  return DseLineString::to(new dse::LineSegment());
}

void dse_line_string_free(DseLineString* line_string) {
  delete line_string->from();
}

void dse_line_string_reset(DseLineString* line_string) {
  line_string->reset();
}

void dse_line_string_reserve(DseLineString* line_string,
                             cass_uint32_t num_points) {
  line_string->reserve(num_points);
}

CassError dse_line_string_add_point(DseLineString* line_string,
                                    cass_double_t x, cass_double_t y) {
  line_string->add_point(x, y);
  return CASS_OK;
}

CassError dse_line_string_finish(DseLineString* line_string) {
  return line_string->finish();
}

DseLineStringIterator* dse_line_string_iterator_new() {
  return DseLineStringIterator::to(new dse::LineSegmentIterator());
}

void dse_line_string_iterator_free(DseLineStringIterator* iterator) {
  delete iterator->from();
}

CassError dse_line_string_iterator_reset(DseLineStringIterator *iterator, const CassValue *value) {
  size_t size;
  const cass_byte_t* pos;
  dse::WkbByteOrder byte_order;
  cass_uint32_t num_points;

  CassError rc = dse::validate_data_type(value, DSE_LINE_STRING_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_LINE_STRING_HEADER_SIZE) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }
  size -= WKB_LINE_STRING_HEADER_SIZE;

  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_LINESTRING) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  num_points = dse::decode_uint32(pos, byte_order);
  pos += sizeof(cass_uint32_t);

  if (size < 2 * num_points * sizeof(cass_double_t)) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }

  const cass_byte_t* points = pos;
  const cass_byte_t* points_end = pos + size;

  iterator->reset(num_points, points, points_end, byte_order);

  return CASS_OK;
}

cass_uint32_t dse_line_string_iterator_num_points(const DseLineStringIterator* iterator) {
  return iterator->num_points();
}

CassError dse_line_string_iterator_next_point(DseLineStringIterator* iterator,
                                              cass_double_t* x, cass_double_t* y) {
  return iterator->next_point(x, y);
}

} // extern "C"
