# Basics

## Feature Compatibility

* Cassandra 2.0+ is required for parameterized queries, prepared statements,
  paging, and batches. Cassandra 1.2 does NOT support these features.
* Cassandra 2.1+ is required for nested collections, UDTs, tuples, and client-side
  timestamps. Cassandra 1.2 and 2.0 do NOT support these features.

## Datatypes Mapping

<table class="table table-striped table-hover table-condensed">
  <thead>
  <tr>
   <th>Cassandra Type(s)</th>
   <th>Driver Type</th>
   <th>Supported Versions</th>
  </tr>
  </thead>

  <tbody>
  <tr>
   <td><code>int</code></td>
   <td><code>cass_int32_t</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>bigint</code>, <code>counter</code>, <code>timestamp</code></td>
   <td><code>cass_int64_t</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>float</code></td>
   <td><code>cass_float_t</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>double</code></td>
   <td><code>cass_double_t</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>boolean</code></td>
   <td><code>cass_boot_t</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>ascii</code>, <code>text</code>, <code>varchar</code></td>
   <td><code>const char&#42;</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>blob</code>, <code>varint</code></td>
   <td><code>const cass_byte_t&#42;</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>uuid</code>, <code>timeuuid</code></td>
   <td><code>CassUuid</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>inet</code></td>
   <td><code>CassInet</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>decimal</code></td>
   <td><code>const cass_byte_t&#42; (varint) and a cass_int32_t (scale)</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>list</code>, <code>map</code>, <code>set</code></td>
   <td><code>CassCollection</code></td>
   <td>1.2+</td>
  </tr>
  <tr>
   <td><code>tuple</code></td>
   <td><code>CassTuple</code></td>
   <td>2.1+</td>
  </tr>
  <tr>
   <td><code>user-defined type</code></td>
   <td><code>CassUserType</code></td>
   <td>2.1+</td>
  </tr>
  <tr>
   <td><code>tinyint</code></td>
   <td><code>cass_int8_t</code></td>
   <td>2.2+</td>
  </tr>
  <tr>
   <td><code>smallint</code></td>
   <td><code>cass_int16_t</code></td>
   <td>2.2+</td>
  </tr>
  <tr>
   <td><code>date</code></td>
   <td><code>cass_uint32_t</code></td>
   <td>2.2+</td>
  </tr>
  <tr>
   <td><code>time</code></td>
   <td><code>cass_int64_t</code></td>
   <td>2.2+</td>
  </tr>
  </tbody>
</table>

