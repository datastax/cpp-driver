# Basics

## Datatypes Mapping

<table class="table table-striped table-hover table-condensed">
  <thead>
    <tr>
      <th>Driver</th>
      <th>Cassandra</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>cass_int32_t</code></td>
      <td><code>int</code></td>
    </tr>

    <tr>
      <td><code>cass_int64_t</code></td>
      <td><code>bigint</code>, <code>counter</code>, <code>timestamp</code></td>
    </tr>

    <tr>
      <td><code>cass_float_t</code></td>
      <td><code>float</code></td>
    </tr>

    <tr>
      <td><code>cass_double_t</code></td>
      <td><code>double</code></td>
    </tr>

    <tr>
      <td><code>cass_boot_t</code></td>
      <td><code>boolean</code></td>
    </tr>

    <tr>
      <td><code>const char*</code></td>
      <td><code>ascii</code>, <code>text</code>, <code>varchar</code></td>
    </tr>

    <tr>
      <td><code>const cass_byte_t*</code></td>
      <td><code>blob</code>, <code>varint</code></td>
    </tr>

    <tr>
      <td><code>CassUuid</code></td>
      <td><code>uuid</code>, <code>timeuuid</code></td>
    </tr>

    <tr>
      <td><code>CassInet</code></td>
      <td><code>inet</code></td>
    </tr>

    <tr>
      <td><code>const cass_byte_t* (varint) and a cass_int32_t (scale)</code></td>
      <td><code>decimal</code></td>
    </tr>

    <tr>
      <td><code>CassCollection</code></td>
      <td><code>list</code>, <code>map</code>, <code>set</code></td>
    </tr>
  </tbody>
</table>

## Feature Compatibility

Cassandra 2.0+ is required for parameterized queries, prepared statements, paging, and batches. Cassandra 1.2 does NOT support these features.
