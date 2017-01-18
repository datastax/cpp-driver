1.1.0
===========

Features
--------
* Expose wkt parsing logic for public use:
  * `dse_point_from_wkt()`, `dse_point_from_wkt_n()`
  * `dse_line_string_iterator_reset_with_wkt()`,
    `dse_line_string_iterator_reset_with_wkt_n()`
  * `dse_polygon_iterator_reset_with_wkt()`,
    `dse_polygon_iterator_reset_with_wkt_n()`

Other
---------
* [CPP-416] LineString::to_wkt and Polygon::to_wkt now return "LINESTRING
  EMPTY" and "POLYGON EMPTY", respectively, for empty
  objects.
* [CPP-416] WKT parsing for "LINESTRING EMPTY" and "POLYGON EMPTY" now
* succeeds.

1.0.0
===========
October 20, 2016

Features
--------
* Added a host targeting policy for graph analytics queries. Graph queries with
  the source "a" will attempt to be routed directly to the analytics master
  node.
* Added geometric types support for graph queries.

1.0.0-rc1
===========
June 24, 2016

Features
--------
* Added a graph option to set graph query request timeout (default: no timeout)
* Added graph options to set the consistency level of graph query requests
* Added the ability to set the timestamp on graph statements

Other
--------
* Changed the default graph source from "default" to "g"
