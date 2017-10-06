/*
  Copyright (c) 2017 DataStax, Inc.

  This software can be used solely with DataStax Enterprise. Please consult the
  license at http://www.datastax.com/terms/datastax-dse-driver-license-terms
*/

#include "dse_integration.hpp"
#include "options.hpp"

#include <rapidjson/rapidjson.h>

#define GRAPH_DATA_TYPE_CREATE_FORMAT "schema.propertyKey(property_name).%s().create();" \
  "schema.vertexLabel(vertex_label).properties(property_name).create();" \
  "schema.vertexLabel(vertex_label).index(property_name + 'Index').secondary().by(property_name).add();"

#define GRAPH_DATA_TYPE_INSERT "g.addV(label, vertex_label, property_name, value_field);"

#define GRAPH_DATA_TYPE_SELECT "g.V().hasLabel(vertex_label).has(property_name, value_field).next();"

/**
 * Graph data type integration tests
 *
 * @dse_version 5.0.0
 */
class GraphDataTypeTest : public DseIntegration {
public:

  /**
   * Pair containing values and expected return values
   */
  typedef std::pair<std::vector<std::string>, std::vector<std::string> > ValuesExpected;

  void SetUp() {
    CHECK_VERSION(5.0.0);

    // Call the parent setup function
    dse_workload_.push_back(CCM::DSE_WORKLOAD_GRAPH);
    DseIntegration::SetUp();

    // Create the graph
    create_graph();
    CHECK_FAILURE;

    // Create the default options for the graph data type integration tests
    options_.set_name(test_name_);
  }

protected:
  /**
   * Perform the data type test for multiple data types.
   *
   * This method will create, insert, and select the data type value while
   * validating all aspects of the operation.
   *
   * @param data_type DSE graph data type
   * @param values The values to insert and validate
   * @param expected_values The server may coerce the values into different
   *                        representation of the value; this will allow the
   *                        validation of those coercions
   */
  template<typename T>
  void perform_data_type_test(const std::string& data_type,
    std::vector<T> values,
    std::vector<T> expected_values = std::vector<T>()) {
    // Determine if the values being validated are the same as the insert values
    if (expected_values.empty()) {
      expected_values = values;
    }

    // Ensure the values are of the same size
    ASSERT_EQ(values.size(), expected_values.size())
      << "Insert and expected vectors must be equal in size";

    // Iterate over the values and perform the test operations
    for (size_t i = 0; i < values.size(); ++i) {
      T value = values[i];
      T expected = expected_values[i];
      test::driver::DseGraphObject object = create_object<T>(value);
      CHECK_FAILURE;

      // Create the data type
      create(data_type, object);
      CHECK_FAILURE;

      // Insert and validate the data type
      test::driver::DseGraphResultSet result_set = insert(object);
      CHECK_FAILURE;
      test::driver::DseGraphResult result = get_data_type_value(result_set);
      CHECK_FAILURE;
      ASSERT_TRUE(result.is_type<T>());
      ASSERT_EQ(expected, result.value<T>());

      // Select and validate the data type
      result_set = select(object);
      CHECK_FAILURE;
      result = get_data_type_value(result_set);
      ASSERT_TRUE(result.is_type<T>());
      ASSERT_EQ(expected, result.value<T>());
    }
  }

private:
  /**
   * Graph options for the data type integration tests
   */
  test::driver::DseGraphOptions options_;
  /**
   * Property name being generated/used
   */
  std::string property_name_;
  /**
   * Vertex label being generated/used
   */
  std::string vertex_label_;

  /**
   * Create the graph object with the specified value for the `value_field`
   *
   * @param value Value to assign to `value_field`
   * @return DSE graph object to apply to DSE graph statements
   */
  template<typename T>
  test::driver::DseGraphObject create_object(T value) {
    test::driver::DseGraphObject object;

    // Initialize the property and vertex labels
    std::vector<std::string> uuid_octets = explode(uuid_generator_.generate_timeuuid().str(), '-');
    property_name_ = "property_" + uuid_octets[0];
    vertex_label_ = "vertex_" + uuid_octets[0];

    // Apply the labels and values
    object.add<std::string>("property_name", property_name_);
    object.add<std::string>("vertex_label", vertex_label_);
    object.add<T>("value_field", value);

    return object;
  }

  /**
   * Create the schema for the graph to insert data type into
   *
   * @param data_type Data type being created
   * @param object DSE graph object for the named parameters
   */
  void create(const std::string& data_type, test::driver::DseGraphObject object) {
    // Determine if data type is geospatial and append withGeoBounds
    std::string type = data_type;
    if (server_version_ >= "5.1.0" &&
      (type.compare("Linestring") == 0 ||
      type.compare("Point") == 0 ||
      type.compare("Polygon") == 0)) {
      type += "().withGeoBounds";
    }

    // Create and execute the statement
    test::driver::DseGraphStatement statement(
      format_string(GRAPH_DATA_TYPE_CREATE_FORMAT, type.c_str()), options_);
    statement.bind(object);
    CHECK_FAILURE;
    dse_session_.execute(statement);
  }

  /**
   * Insert the data type value into the graph
   *
   * @param object DSE graph object for the named parameters (includes value)
   * @return DSE graph result set to validate insert
   */
  test::driver::DseGraphResultSet insert(test::driver::DseGraphObject object) {
    test::driver::DseGraphStatement statement(GRAPH_DATA_TYPE_INSERT, options_);
    statement.bind(object);
    return dse_session_.execute(statement);
  }

  /**
   * Retrieve/Select the data type value into the graph
   *
   * @param object DSE graph object for the named parameters (includes value)
   * @return DSE graph result set to validate
   */
  test::driver::DseGraphResultSet select(test::driver::DseGraphObject object) {
    test::driver::DseGraphStatement statement(GRAPH_DATA_TYPE_SELECT, options_);
    statement.bind(object);
    return dse_session_.execute(statement);
  }

  /**
   * Get the data type value from a result set.
   *
   * NOTE: This method traverses the DSE graph result set until in gets to
   *       where the value is stored.
   *
   * @param result_set DSE graph result set to get the value from
   * @return DSE graph result which will contain only the data type value
   */
  test::driver::DseGraphResult get_data_type_value(
    test::driver::DseGraphResultSet result_set) {
    EXPECT_EQ(1u, result_set.count());
    test::driver::DseGraphResult result = result_set.next();
    test::driver::DseGraphVertex vertex = result.vertex();
    EXPECT_EQ(vertex_label_, vertex.label().value<std::string>());

    test::driver::DseGraphResult property = vertex.properties();
    EXPECT_EQ(DSE_GRAPH_RESULT_TYPE_OBJECT, property.type());
    EXPECT_EQ(1u, property.member_count());
    EXPECT_EQ(property_name_, property.key(0));
    property = property.member(0);

    EXPECT_EQ(DSE_GRAPH_RESULT_TYPE_ARRAY, property.type());
    EXPECT_TRUE(property.is_type<test::driver::DseGraphArray>());
    property = property.element(0);

    EXPECT_EQ(DSE_GRAPH_RESULT_TYPE_OBJECT, property.type());
    EXPECT_TRUE(property.is_type<test::driver::DseGraphObject>());
    EXPECT_EQ(2u, property.member_count());
    EXPECT_EQ("value", property.key(1));

    // Get the value property and return
    return property.member(1);
  }
};

/**
 * Perform insert and select operations for graph data type `bigint`
 *
 * This test will perform multiple inserts and select operations using the
 * big integer values against a single node cluster using graph.
 *
 * @jira_ticket CPP-352
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Bigint is usable and retrievable
 */
DSE_INTEGRATION_TEST_F(GraphDataTypeTest, BigInteger) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Create the values being tested
  std::vector<test::driver::BigInteger> values;
  values.push_back(test::driver::BigInteger::max());
  values.push_back(test::driver::BigInteger::min());
  values.push_back(test::driver::BigInteger(static_cast<cass_int64_t>(0)));

  // Perform the test operations for all the values in the data type
  perform_data_type_test("Bigint", values);
}

/**
 * Perform insert and select operations for graph data type `decimal`, `double`,
 * and `float`
 *
 * This test will perform multiple inserts and select operations using the
 * decimal, double, and float values against a single node cluster using graph.
 *
 * @jira_ticket CPP-352
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Decimal, double, and float are usable and retrievable
 */
DSE_INTEGRATION_TEST_F(GraphDataTypeTest, DecimalDoubleFloat) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Create the decimal values
  std::vector<test::driver::Double> decimals;
  decimals.push_back(test::driver::Double(8675309.9998));
  decimals.push_back(test::driver::Double(3.14159265359));

  // Create the double values
  std::vector<test::driver::Double> doubles;
  doubles.push_back(test::driver::Double(123456.123456));
  doubles.push_back(test::driver::Double(456789.456789));

  // Create the float values
  std::vector<test::driver::Double> floats;
  floats.push_back(test::driver::Double(123.123));
  floats.push_back(test::driver::Double(456.456));

  // Create the values being tested (data type, [value])
  std::map<std::string, std::vector<test::driver::Double> > values;
  values.insert(std::make_pair("Decimal", decimals));
  values.insert(std::make_pair("Double", doubles));
  values.insert(std::make_pair("Float", floats));

  // Iterate over all the values and perform the test operations
  for (std::map<std::string, std::vector<test::driver::Double> >::iterator iterator = values.begin();
    iterator != values.end(); ++iterator) {
    TEST_LOG("Testing data type " << iterator->first);
    perform_data_type_test<test::driver::Double>(iterator->first, iterator->second);
  }
}

/**
 * Perform insert and select operations for graph data type `int`, `smallint`,
 * and `varint`
 *
 * This test will perform multiple inserts and select operations using the
 * integer, small integer, and varint values against a single node cluster using
 * graph.
 *
 * @jira_ticket CPP-352
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Int, smallint, and varint are usable and retrievable
 */
DSE_INTEGRATION_TEST_F(GraphDataTypeTest, IntegerSmallIntegerVarint) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Create the integer values
  std::vector<test::driver::Integer> integers;
  integers.push_back(test::driver::Integer::max());
  integers.push_back(test::driver::Integer::min());
  integers.push_back(test::driver::Integer(0));

  // Create the small integer values
  std::vector<test::driver::Integer> small_integers;
  small_integers.push_back(test::driver::SmallInteger::max().str());
  small_integers.push_back(test::driver::SmallInteger::min().str());

  // Create the values being tested (data type, [value])
  std::map<std::string, std::vector<test::driver::Integer> > values;
  values.insert(std::make_pair("Int", integers));
  values.insert(std::make_pair("Smallint", small_integers));
  values.insert(std::make_pair("Varint", integers));

  // Iterate over all the values and perform the test operations
  for (std::map<std::string, std::vector<test::driver::Integer> >::iterator iterator = values.begin();
    iterator != values.end(); ++iterator) {
    TEST_LOG("Testing data type " << iterator->first);
    perform_data_type_test<test::driver::Integer>(iterator->first, iterator->second);
  }
}

/**
 * Perform insert and select operations for graph data type `text`
 *
 * This test will perform multiple inserts and select operations using the
 * text values against a single node cluster using graph.
 *
 * @jira_ticket CPP-352
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result Text are usable and retrievable
 */
DSE_INTEGRATION_TEST_F(GraphDataTypeTest, Text) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Create the values being tested
  std::vector<test::driver::Varchar> values;
  values.push_back(test::driver::Varchar(
    "The quick brown fox jumps over the lazy dog"));
  values.push_back(test::driver::Varchar("Hello World!"));
  values.push_back(test::driver::Varchar("DataStax C/C++ DSE Driver"));

  // Perform the test operations for all the values in the data type
  perform_data_type_test<test::driver::Varchar>("Text", values);
}

/**
 * Perform insert and select operations for graph data type `blob`, `duration`,
 * `inet`, `linestring`, `point`, `polygon`, `uuid`, and `timestamp`
 *
 * This test will perform multiple inserts and select operations using the
 * blob, duration, inet, linestring, point, polygon, uuid, and timestamp values
 * against a single node cluster using graph.
 *
 * NOTE: All these values return string results from the driver.
 *
 * @jira_ticket CPP-352
 * @test_category dse:graph
 * @since 1.0.0
 * @expected_result String results from the driver are usable and retrievable
 */
DSE_INTEGRATION_TEST_F(GraphDataTypeTest, StringResults) {
  CHECK_VERSION(5.0.0);
  CHECK_FAILURE;

  // Create blob values
  std::vector<std::string> blobs;
  blobs.push_back("RGF0YVN0YXggQy9DKysgRFNFIERyaXZlcg=="); // DataStax C/C++ DSE Driver [base64]

  // Create inet values
  std::vector<std::string> inets;
  inets.push_back("127.0.0.1");
  inets.push_back("0:0:0:0:0:0:0:1");
  inets.push_back("2001:db8:85a3:0:0:8a2e:370:7334");

  // Create UUID values
  std::vector<std::string> uuids;
  uuids.push_back(test::driver::Uuid::max().str());
  uuids.push_back(test::driver::Uuid::min().str());
  uuids.push_back(uuid_generator_.generate_random_uuid().str());
  uuids.push_back(uuid_generator_.generate_timeuuid().str());

  // Create the values being tested (data type, [value])
  std::map<std::string, std::vector<std::string> > values;
  values.insert(std::make_pair("Blob", blobs));
  values.insert(std::make_pair("Inet", inets));
  values.insert(std::make_pair("Uuid", uuids));

  /*
   * Test data types with different expected values
   */
   // Create duration values
  std::vector<std::string> durations;
  durations.push_back("5 s");
  durations.push_back("5 seconds");
  durations.push_back("1 minute");
  durations.push_back("P1DT1H4M1S");
  durations.push_back("P2DT3H4M5S");
  std::vector<std::string> durations_expected;
  durations_expected.push_back("PT5S");
  durations_expected.push_back("PT5S");
  durations_expected.push_back("PT1M");
  durations_expected.push_back("PT25H4M1S");
  durations_expected.push_back("PT51H4M5S");

  // Create line string values (remove tick marks from CQL value)
  std::vector<std::string> line_strings;
  line_strings.push_back(replace_all(
    test::driver::DseLineString("0.0 0.0, 1.0 1.0").cql_value(),
    "'", ""));
  line_strings.push_back(replace_all(
    test::driver::DseLineString("1.0 3.0, 2.0 6.0, 3.0 9.0").cql_value(),
    "'", ""));
  line_strings.push_back(replace_all(
    test::driver::DseLineString("-1.2 -90.0, 0.99 3.0").cql_value(),
    "'", ""));
  std::vector<std::string> line_strings_expected;
  line_strings_expected.push_back("LINESTRING (0 0, 1 1)");
  line_strings_expected.push_back("LINESTRING (1 3, 2 6, 3 9)");
  line_strings_expected.push_back("LINESTRING (-1.2 -90, 0.99 3)");

  // Create point values (remove tick marks from CQL value)
  std::vector<std::string> points;
  points.push_back(replace_all(
    test::driver::DsePoint(0.0, 0.0).cql_value(),
    "'", ""));
  points.push_back(replace_all(
    test::driver::DsePoint(2.0, 4.0).cql_value(),
    "'", ""));
  points.push_back(replace_all(
    test::driver::DsePoint(-1.2, -90.0).cql_value(),
    "'", ""));
  std::vector<std::string> points_expected;
  points_expected.push_back("POINT (0 0)");
  points_expected.push_back("POINT (2 4)");
  points_expected.push_back("POINT (-1.2 -90)");

  // Create polygon values (remove tick marks from CQL value)
  std::vector<std::string> polygons;
  polygons.push_back(replace_all(
    test::driver::DsePolygon("(1.0 3.0, 3.0 1.0, 3.0 6.0, 1.0 3.0)").cql_value(),
    "'", ""));
  polygons.push_back(replace_all(
    test::driver::DsePolygon("(0.0 10.0, 10.0 0.0, 10.0 10.0, 0.0 10.0), \
                              (6.0 7.0, 3.0 9.0, 9.0 9.0, 6.0 7.0)").cql_value(),
    "'", ""));
  std::vector<std::string> polygons_expected;
  polygons_expected.push_back("POLYGON ((1 3, 3 1, 3 6, 1 3))");
  polygons_expected.push_back("POLYGON ((0 10, 10 0, 10 10, 0 10), " \
                                      "(6 7, 3 9, 9 9, 6 7))");

  // Create timestamp values
  std::vector<std::string> timestamps;
  timestamps.push_back("1000");
  timestamps.push_back("1270110600000");
  std::vector<std::string> timestamps_expected;
  timestamps_expected.push_back("1970-01-01T00:00:01Z");
  timestamps_expected.push_back("2010-04-01T08:30:00Z");

  // Create the valued being tested (data type, [[value], [expected]]]
  std::map<std::string, ValuesExpected> values_expected;
  values_expected.insert(std::make_pair("Duration",
    std::make_pair(durations, durations_expected)));
  values_expected.insert(std::make_pair("Linestring",
    std::make_pair(line_strings, line_strings_expected)));
  values_expected.insert(std::make_pair("Point",
    std::make_pair(points, points_expected)));
  values_expected.insert(std::make_pair("Polygon",
    std::make_pair(polygons, polygons_expected)));
  values_expected.insert(std::make_pair("Timestamp",
    std::make_pair(timestamps, timestamps_expected)));

  // Iterate over all the values and perform the test operations
  for (std::map<std::string, std::vector<std::string> >::iterator iterator = values.begin();
    iterator != values.end(); ++iterator) {
    TEST_LOG("Testing data type " << iterator->first);
    perform_data_type_test<std::string>(iterator->first, iterator->second);
  }
  for (std::map<std::string, ValuesExpected>::iterator iterator = values_expected.begin();
    iterator != values_expected.end(); ++iterator) {
    TEST_LOG("Testing data type " << iterator->first);
    perform_data_type_test<std::string>(iterator->first,
      iterator->second.first, iterator->second.second);
  }
}
