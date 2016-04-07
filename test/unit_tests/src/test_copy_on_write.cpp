/*
  Copyright (c) 2014-2016 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifdef STAND_ALONE
#   define BOOST_TEST_MODULE cassandra
#endif

#include "copy_on_write_ptr.hpp"
#include "ref_counted.hpp"

#include <boost/test/unit_test.hpp>
#include <map>
#include <string>

BOOST_AUTO_TEST_SUITE(copy_on_write)

BOOST_AUTO_TEST_CASE(simple)
{
  std::vector<int>* ptr = new std::vector<int>;
  cass::CopyOnWritePtr<std::vector<int> > vec(ptr);

  // Only a single reference so no copy should be made
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() == ptr);
  vec->push_back(1);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() == ptr);

  // Make const reference to object
  const cass::CopyOnWritePtr<std::vector<int> > const_vec(vec);
  BOOST_CHECK((*const_vec)[0] == 1);
  BOOST_CHECK(const_vec.operator->() == ptr);

  // Force copy to be made
  vec->push_back(2);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<std::vector<int> >&>(vec).operator->() != ptr);
  BOOST_CHECK(const_vec.operator->() == ptr);
}

struct Table : public cass::RefCounted<Table> {
  typedef cass::SharedRefPtr<Table> Ptr;
  typedef std::map<std::string, Table::Ptr> Map;

  Table(const std::string name)
    : name(name) { }

  std::string name;
private:
  DISALLOW_COPY_AND_ASSIGN(Table);
};

struct Keyspace {
  typedef std::map<std::string, Keyspace> Map;

  Keyspace()
    : tables(new Table::Map) { }

  void add_table(const Table::Ptr& table) {
    (*tables)[table->name] = table;
  }

  cass::CopyOnWritePtr<Table::Map> tables;
};

struct Metadata {
  Metadata()
    : keyspaces(new Keyspace::Map) { }

  Keyspace* get_or_create(const std::string& name) {
    return &(*keyspaces)[name];
  }

  cass::CopyOnWritePtr<Keyspace::Map> keyspaces;
};

BOOST_AUTO_TEST_CASE(nested)
{
  Metadata m1;
  Keyspace* k1 = m1.get_or_create("k1");
  k1->add_table(Table::Ptr(new Table("t1")));
  k1->add_table(Table::Ptr(new Table("t2")));

  Keyspace* k2 = m1.get_or_create("k2");
  k2->add_table(Table::Ptr(new Table("t1")));
  k2->add_table(Table::Ptr(new Table("t2")));

  Metadata m2 = m1;

  m1.get_or_create("k1")->add_table(Table::Ptr(new Table("t2")));
}

BOOST_AUTO_TEST_SUITE_END()

