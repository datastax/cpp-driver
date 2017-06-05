/*
  Copyright (c) 2014-2017 DataStax

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
#include "map.hpp"
#include "ref_counted.hpp"
#include "string.hpp"
#include "vector.hpp"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(copy_on_write)

BOOST_AUTO_TEST_CASE(simple)
{
  cass::Vector<int>* ptr = new cass::Vector<int>;
  cass::CopyOnWritePtr<cass::Vector<int> > vec(ptr);

  // Only a single reference so no copy should be made
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<cass::Vector<int> >&>(vec).operator->() == ptr);
  vec->push_back(1);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<cass::Vector<int> >&>(vec).operator->() == ptr);

  // Make const reference to object
  const cass::CopyOnWritePtr<cass::Vector<int> > const_vec(vec);
  BOOST_CHECK((*const_vec)[0] == 1);
  BOOST_CHECK(const_vec.operator->() == ptr);

  // Force copy to be made
  vec->push_back(2);
  BOOST_CHECK(static_cast<const cass::CopyOnWritePtr<cass::Vector<int> >&>(vec).operator->() != ptr);
  BOOST_CHECK(const_vec.operator->() == ptr);
}

struct Table : public cass::RefCounted<Table> {
  typedef cass::SharedRefPtr<Table> Ptr;
  typedef cass::Map<cass::String, Table::Ptr> Map;

  Table(const cass::String name)
    : name(name) { }

  cass::String name;
private:
  DISALLOW_COPY_AND_ASSIGN(Table);
};

struct Keyspace {
  typedef cass::Map<cass::String, Keyspace> Map;

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

  Keyspace* get_or_create(const cass::String& name) {
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

