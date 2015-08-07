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
      <th>Driver</th>
      <th>Cassandra</th>
      <th>Supported Versions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>cass_int32_t</code></td>
      <td><code>int</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>cass_int64_t</code></td>
      <td><code>bigint</code>, <code>counter</code>, <code>timestamp</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>cass_float_t</code></td>
      <td><code>float</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>cass_double_t</code></td>
      <td><code>double</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>cass_boot_t</code></td>
      <td><code>boolean</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>const char*</code></td>
      <td><code>ascii</code>, <code>text</code>, <code>varchar</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>const cass_byte_t*</code></td>
      <td><code>blob</code>, <code>varint</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>CassUuid</code></td>
      <td><code>uuid</code>, <code>timeuuid</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>CassInet</code></td>
      <td><code>inet</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>const cass_byte_t* (varint) and a cass_int32_t (scale)</code></td>
      <td><code>decimal</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>CassCollection</code></td>
      <td><code>list</code>, <code>map</code>, <code>set</code></td>
      <td>1.2+</td>
    </tr>

    <tr>
      <td><code>CassTuple</code></td>
      <td><code>tuple</code></td>
      <td>2.1+</td>
    </tr>

    <tr>
      <td><code>CassUserType</code></td>
      <td><code>type</code></td>
      <td>2.1+</td>
    </tr>

  </tbody>
</table>

