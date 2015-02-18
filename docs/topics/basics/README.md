# Basics

## Feature Compatibility

Cassandra 2.0+ is required for parameterized queries, prepared statements, paging, and batches. Cassandra 1.2 does NOT support these features.

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
      <td><code>CassString</code></td>
      <td><code>ascii</code>, <code>text</code>, <code>varchar</code></td>
    </tr>

    <tr>
      <td><code>CassBytes</code></td>
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
      <td><code>CassDecimal</code></td>
      <td><code>decimal</code></td>
    </tr>

    <tr>
      <td><code>CassCollection</code></td>
      <td><code>list</code>, <code>map</code>, <code>set</code></td>
    </tr>
  </tbody>
</table>

## Using `varint` and `decimal`

Cassandra and the [java-driver](https://github.com/datastax/java-driver) are able to map the `varint` and `decimal` types to the [`BigInteger`](http://docs.oracle.com/javase/8/docs/api/java/math/BigInteger.html) and [`BigDecimal`](http://docs.oracle.com/javase/8/docs/api/java/math/BigDecimal.html) types. C and C++ doesn't have built-in library support for using those values therefore it returns the byte representations for those types. These byte representations can be used directly or in conjunction with arbitrary precision math libraries such as [Boost's Multiprecision Library](http://www.boost.org/doc/libs/1_57_0/libs/multiprecision/doc/html/boost_multiprecision/intro.html) or [GMP](https://gmplib.org/).

<table class="table table-striped table-hover table-condensed">
  <thead>
    <tr>
      <th>Cassandra</th>
      <th>Driver</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>varint</code></td>
      <td><code>CassBytes</code></td>
      <td>A byte array of the two's-complement representation of a signed, big-endian integer: the most significant byte is at the zeroth index.</td>
    </tr>

    <tr>
      <td><code>decimal</code></td>
      <td><code>CassDecimal</code></td>
      <td>
        <p>An unscaled value represented by a signed, big-endian byte array in two's-complement (same as the `varint` representation) and signed scale value represetned by a signed 32-bit integer. The value of the decimal is described by: <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAKgAAAATCAAAAAAhLRneAAAABGdBTUEAALGPC/xhBQAAAAJiS0dEAP+Hj8y/AAAACXBIWXMAAABkAAAAZAAPlsXdAAAAB3RJTUUH3wIMCwAdcPlrugAABhBJREFUSMedVktslFUUPjP/DMREaAdoNwb6oBoTEwOliokLa2dajRs1tQRYGEJpKyExPjqdR4cFURCQjYm2tTQao6bGqa4kfVAq4EZLIaAsClQe6sYC0wooM/99HM+5d6aMDMS0f/q4//3vPfc73znnOxdw/o/SKMxAi7xZGiuaKljtIkpeaz5Ja0Dad52b4O+Sx8a0NOYzbI6W0BS/w7xhCo2uROXibVQq7yB0tWZUd3tFUJXSdJyis+e8Iwv0K9kL3uVaT6U0a4Rdpdi+VvSqFwBUIuGxxGnMA6bkfTYoNcc48qEKlaB9BMACt5Qr7TKp5kkjvTAbiYg2G2mTnj+jko/WKDkkKj/UWgqBBXBtFLXhjWlkBk146cfld8VMa/I+w3E2sTJBpxXBpB2yiwsASlvTOud6Hv4crLufjEldYeAJ65w0/0QWhdBnN4LniYvu9LZSWH0YVXcZdLx1ksI9qfBkrVMxQWfOG6i2+dMYVBw4eYdByolBGC1MAOKK+WJYttik1FlLKkfYqncQv8aZp9rO6/HfxUDJMdUU+FmNl6O8Un/F7ayUCykmrlapy6N0eDoflk7jfrh+T88Utm94GOoJ6N/o3theBFVRTkjFhc7u6hU9TPLWWm3YLutSeBgk7t8hcesE5S6BzCyg6s3R3j22kuagUnT024/KQnkycfY92+ENMaUKH1/WeyQMcVTD4AB4fRBDPdZUtOlLXJ5kfsUl74zG7gbE1m6FgbqGYLCeXAKjH9kSsXpnZdLmPM4JoMl+nRPLIRjmVyszuSjq55uM6lhVUAazmsMLQbOwF/robxtc1Dpbi9a5CKAzaEYjPsLT2oFq3SlUHnMuJQ8QmjdLeXyAyHW7Qj+9Ap43+IAzjYHFEDxBwxNNxVASo6lUWwnUngn9gJl9wDV1vmUZrA4zpD8TxVAzDlEUyhBNEKDjv5KVBRrysECNQdT6wjWORydVqjWIa5puqamr+hJ85r4H7bfQk9K4MnETJyZZR2ntxjoyL1tqiK4N5U93Db0OH6OaLA59PjrSMaNEn6dhYKwr7GJqbXXP0KaH4DL5X0lCeKo01D8adiKIV9eXdR/pWOGMpA2LGUz+iotj+uBfeXKVBbpkPYVITXsbTSvjsGVUZDl4t94U40EP1PyCbgwCiZdL0xh0DuGpaq+/fhZtMVXHOPrrt1ALqaqcpq2Q+AfjcIsTXeHUilZlzhfbK69rnIUArW6oQ0w98BrnSUtAp8MlKTo3CBe4m3C0K4ri0LQGkqKAUafWZBSErEoxvXcyPc1Oau4INpuoXCkPWVwp9C5CD4cL9gk969lNBTbr6UbdBy9OmDO3B6Y5j6Se8vW5xIY/SPR7w1ru8H0xNjpyvBnwD88uPjK21Fona9c6H/RB3en83n8foGjV1PRXV1rQMpvggrM926aJ0e+JYVRDzjDid3CaII9SpQh5oAaqOslmWRutZGnvgbNskKrUnYJPEcu5agEWl4mD3ilBUvXSC7l26V5uBh+UJd3CYioI/W00kKWtXZ1r0orT3XItVUYTULHH4Sa330tXg6iffdptNEvLGxHYq9ATV8aGG/crAjwASYnH4UclFkVVthO1A9ucLYrQd9sAH6n4yP/qZqevMPR3FxPmRCODbsZeHbTMBt50QdeOCFMY2FxlNb1tfobpaH1SWk+uO++iDmzLilCEUhD1Gu8Ub6HPS3YQKkEHyL3OFEVoi69fWJnSODiDEMHB/KvAfeVJs/YpqxbcxYwAKpsMWllaWZ6w2998pD8YWHWUohnl2bpmghJPDveuW0b11gnxkaFIL+IFb3VyYN1S6JMYg28uqZ3QMjaSjH+A+tyitQNjjVXO5J1Ao3quJ9sdaPL2xOiIp+bQsRRRkhP8eT4s+O3gVH4S9u/Ca54POV1gJ+r368Epbj1HuEV0tcdff56q72AFlMe+hU0of1sF/SgTj4G3YsNpyqWvymFloiuI+ZcnEz9bHALrqQdRPo9qN9dC5w9Um96QybYlU7NUbjafTRjp1utqc7nIpg/XFmdnhq6MNifNTVKYO2bu0XPXVaUMYiVyneheffZ/n38BY6mn1E08YtIAAAAldEVYdGRhdGU6Y3JlYXRlADIwMTUtMDItMTJUMTE6MDA6MjktMDY6MDAhVIZCAAAAJXRFWHRkYXRlOm1vZGlmeQAyMDE1LTAyLTEyVDExOjAwOjI5LTA2OjAwUAk+/gAAAABJRU5ErkJggg==" />.</p>
        <ul>
          <li>Example: 0.19 is represented by, unscaled = 19, scale = 2</li>
        </ul>
      </td>
    </tr>
  </tbody>
</table>

