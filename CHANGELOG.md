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
