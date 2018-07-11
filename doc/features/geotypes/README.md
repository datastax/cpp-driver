# Geospatial types

DSE 5.0 comes with additional types to represent geospatial data: `PointType`,
`LineStringType` and `PolygonType`. These types can be added directly using CQL
or the C/C++ DSE driver.

```
cqlsh> CREATE TABLE IF NOT EXISTS geotypes (key text PRIMARY KEY, point 'PointType', linestring 'LineStringType', polygon 'PolygonType');
cqlsh> INSERT INTO geotypes (key, point) VALUES ('point', 'POINT(42 3.14159)');
cqlsh> INSERT INTO geotypes (key, linestring) VALUES ('linestring', 'LINESTRING(0 0, 1 1)');
cqlsh> INSERT INTO geotypes (key, linestring) VALUES ('polygon', 'POLYGON((0 0, 1 0, 1 1, 0 0))');
```

These geospatial types can be also be used directly from C/C++ types:

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, point) VALUES (?, ?)", 2);

cass_statement_bind_string(statement, 0, "point");

/* Bind a point using with the point's components */
cass_statement_bind_dse_point(statement, 1, 42, 3.141459);

/* Execute statement */
```

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, linestring) VALUES (?, ?)", 2);

/* Construct the line string */
DseLineString* line_string = dse_line_string_new();

/* Optionally reserve space for the points */
dse_line_string_reserve(line_string, 2);

/* Add points to the line string */
dse_line_string_add_point(line_string, 0, 0);
dse_line_string_add_point(line_string, 1, 1);

/* Tell the line string we are not going to add any more points */
dse_line_string_finish(line_string);

cass_statement_bind_string(statement, 0, "linestring");

/* Bind the line string */
cass_statement_bind_dse_line_string(statement, 1, line_string);

/* Execute statement */
```

```c
CassStatement* statement =
  cass_statement_new("INSERT INTO examples.geotypes (key, polygon) VALUES (?, ?)", 2);

/* Construct the polygon */
DsePolygon* polygon = dse_polygon_new();

/* A start a new ring */
dse_polygon_start_ring(polygon);

/* Add points to the current ring */
dse_polygon_add_point(polygon, 0, 0);
dse_polygon_add_point(polygon, 1, 0);
dse_polygon_add_point(polygon, 1, 1);
dse_polygon_add_point(polygon, 0, 0);

/* Tell the polygon string we are not going to add any more rings or points */
dse_polygon_finish(polygon);

cass_statement_bind_string(statement, 0, "key");

/* Bind the polygon */
cass_statement_bind_dse_polygon(statement, 1, polygon);

/* Execute statement */
```
