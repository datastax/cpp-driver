/*
  Copyright (c) 2016 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "external_types.hpp"
#include "polygon.hpp"
#include "validate.hpp"

extern "C" {

DsePolygon* dse_polygon_new() {
  return DsePolygon::to(new dse::Polygon());
}

void dse_polygon_free(DsePolygon* polygon) {
  delete polygon->from();
}

void dse_polygon_reset(DsePolygon* polygon) {
  polygon->reset();
}

void dse_polygon_reserve(DsePolygon* polygon,
                         cass_uint32_t num_rings,
                         cass_uint32_t total_num_points) {
  polygon->reserve(num_rings, total_num_points);
}

CassError dse_polygon_start_ring(DsePolygon* polygon) {
  return polygon->start_ring();
}

CassError dse_polygon_add_point(DsePolygon* polygon,
                                cass_double_t x, cass_double_t y) {
  polygon->add_point(x, y);
  return CASS_OK;
}

CassError dse_polygon_finish(DsePolygon* polygon) {
  return polygon->finish();
}

DsePolygonIterator* dse_polygon_iterator_new() {
  return DsePolygonIterator::to(new dse::PolygonIterator());
}

CassError dse_polygon_iterator_reset(DsePolygonIterator* iterator,
                                     const CassValue* value) {
  size_t size;
  const cass_byte_t* pos;
  dse::WkbByteOrder byte_order;
  cass_uint32_t num_rings;

  CassError rc = dse::validate_data_type(value, DSE_POLYGON_TYPE);
  if (rc != CASS_OK) return rc;

  rc = cass_value_get_bytes(value, &pos, &size);
  if (rc != CASS_OK) return rc;

  if (size < WKB_POLYGON_HEADER_SIZE) {
    return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
  }
  size -= WKB_POLYGON_HEADER_SIZE;

  if (dse::decode_header(pos, &byte_order) != dse::WKB_GEOMETRY_TYPE_POLYGON) {
    return CASS_ERROR_LIB_INVALID_DATA;
  }
  pos += WKB_HEADER_SIZE;

  num_rings = dse::decode_uint32(pos, byte_order);
  pos += sizeof(cass_uint32_t);

  const cass_byte_t* rings = pos;
  const cass_byte_t* rings_end = pos + size;

  for (cass_uint32_t i = 0; i < num_rings; ++i) {
    cass_uint32_t num_points;

    if (size < sizeof(cass_uint32_t)) {
      return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    }
    size -= sizeof(cass_uint32_t);

    num_points = dse::decode_uint32(pos, byte_order);
    pos += sizeof(cass_uint32_t);

    if (size < 2 * num_points * sizeof(cass_double_t)) {
      return CASS_ERROR_LIB_NOT_ENOUGH_DATA;
    }
    size -= 2 * num_points * sizeof(cass_double_t);
  }

  iterator->reset(num_rings, rings, rings_end, byte_order);

  return CASS_OK;
}

void dse_polygon_iterator_free(DsePolygonIterator* iterator) {
  delete iterator->from();
}

cass_uint32_t dse_polygon_iterator_num_rings(const DsePolygonIterator* iterator) {
  return iterator->num_rings();
}

CassError dse_polygon_iterator_next_num_points(DsePolygonIterator* iterator,
                                               cass_uint32_t* num_points) {
  return iterator->next_num_points(num_points);
}

CassError dse_polygon_iterator_next_point(DsePolygonIterator* iterator,
                                          cass_double_t* x, cass_double_t* y) {
  return iterator->next_point(x, y);
}

} // extern "C"
