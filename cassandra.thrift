#!/usr/local/bin/thrift --java --php --py
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# *** PLEASE REMEMBER TO EDIT THE VERSION CONSTANT WHEN MAKING CHANGES ***
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#
# Interface definition for Cassandra Service
#

namespace java org.apache.cassandra.thrift
namespace cpp org.apache.cassandra
namespace csharp Apache.Cassandra
namespace py cassandra
namespace php cassandra
namespace perl Cassandra

# Thrift.rb has a bug where top-level modules that include modules 
# with the same name are not properly referenced, so we can't do
# Cassandra::Cassandra::Client.
namespace rb CassandraThrift

# The API version (NOT the product version), composed as a dot delimited
# string with major, minor, and patch level components.
#
#  - Major: Incremented for backward incompatible changes. An example would
#           be changes to the number or disposition of method arguments.
#  - Minor: Incremented for backward compatible changes. An example would
#           be the addition of a new (optional) method.
#  - Patch: Incremented for bug fixes. The patch level should be increased
#           for every edit that doesn't result in a change to major/minor.
#
# See the Semantic Versioning Specification (SemVer) http://semver.org.
const string VERSION = "6.1.0"

#
# data structures
#

/** Basic unit of data within a ColumnFamily.
 * @param name, the name by which this column is set and retrieved.  Maximum 64KB long.
 * @param value. The data associated with the name.  Maximum 2GB long, but in practice you should limit it to small numbers of MB (since Thrift must read the full value into memory to operate on it).
 * @param timestamp. The highest timestamp associated with the given column name is the one whose value the system will converge to.  No other assumptions are made about what the timestamp represents, but using microseconds-since-epoch is customary.
 * @param ttl. An optional, positive delay (in seconds) after which the column will be automatically deleted. 
 */
struct Column {
   1: required binary name,
   2: required binary value,
   3: required i64 timestamp,
   4: optional i32 ttl,
}

/** A named list of columns.
 * @param name. see Column.name.
 * @param columns. A collection of standard Columns.  The columns within a super column are defined in an adhoc manner.
 *                 Columns within a super column do not have to have matching structures (similarly named child columns).
 */
struct SuperColumn {
   1: required binary name,
   2: required list<Column> columns,
}

/**
    Methods for fetching rows/records from Cassandra will return either a single instance of ColumnOrSuperColumn or a list
    of ColumnOrSuperColumns (get_slice()). If you're looking up a SuperColumn (or list of SuperColumns) then the resulting
    instances of ColumnOrSuperColumn will have the requested SuperColumn in the attribute super_column. For queries resulting
    in Columns, those values will be in the attribute column. This change was made between 0.3 and 0.4 to standardize on
    single query methods that may return either a SuperColumn or Column.

    @param column. The Column returned by get() or get_slice().
    @param super_column. The SuperColumn returned by get() or get_slice().
 */
struct ColumnOrSuperColumn {
    1: optional Column column,
    2: optional SuperColumn super_column,
}


#
# Exceptions
# (note that internal server errors will raise a TApplicationException, courtesy of Thrift)
#

/** A specific column was requested that does not exist. */
exception NotFoundException {
}

/** Invalid request could mean keyspace or column family does not exist, required parameters are missing, or a parameter is malformed. 
    why contains an associated error message.
*/
exception InvalidRequestException {
    1: required string why
}

/** Not all the replicas required could be created and/or read. */
exception UnavailableException {
}

/** RPC timeout was exceeded.  either a node failed mid-operation, or load was too high, or the requested op was too large. */
exception TimedOutException {
}

/** invalid authentication request (invalid keyspace, user does not exist, or credentials invalid) */
exception AuthenticationException {
    1: required string why
}

/** invalid authorization request (user does not have access to keyspace) */
exception AuthorizationException {
    1: required string why
}


#
# service api
#
/** The ConsistencyLevel is an enum that controls both read and write behavior based on <ReplicationFactor> in your
 * storage-conf.xml. The different consistency levels have different meanings, depending on if you're doing a write or read
 * operation. Note that if W + R > ReplicationFactor, where W is the number of nodes to block for on write, and R
 * the number to block for on reads, you will have strongly consistent behavior; that is, readers will always see the most
 * recent write. Of these, the most interesting is to do QUORUM reads and writes, which gives you consistency while still
 * allowing availability in the face of node failures up to half of <ReplicationFactor>. Of course if latency is more
 * important than consistency then you can use lower values for either or both.
 *
 * Write:
 *      ZERO    Ensure nothing. A write happens asynchronously in background
 *      ANY     Ensure that the write has been written once somewhere, including possibly being hinted in a non-target node.
 *      ONE     Ensure that the write has been written to at least 1 node's commit log and memory table before responding to the client.
 *      QUORUM  Ensure that the write has been written to <ReplicationFactor> / 2 + 1 nodes before responding to the client.
 *      ALL     Ensure that the write is written to <code>&lt;ReplicationFactor&gt;</code> nodes before responding to the client.
 *
 * Read:
 *      ZERO    Not supported, because it doesn't make sense.
 *      ANY     Not supported. You probably want ONE instead.
 *      ONE     Will return the record returned by the first node to respond. A consistency check is always done in a
 *              background thread to fix any consistency issues when ConsistencyLevel.ONE is used. This means subsequent
 *              calls will have correct data even if the initial read gets an older value. (This is called 'read repair'.)
 *      QUORUM  Will query all storage nodes and return the record with the most recent timestamp once it has at least a
 *              majority of replicas reported. Again, the remaining replicas will be checked in the background.
 *      ALL     Not yet supported, but we plan to eventually.
*/
enum ConsistencyLevel {
    ZERO = 0,
    ONE = 1,
    QUORUM = 2,
    DCQUORUM = 3,
    DCQUORUMSYNC = 4,
    ALL = 5,
    ANY = 6,
}

/**
    ColumnParent is used when selecting groups of columns from the same ColumnFamily. In directory structure terms, imagine
    ColumnParent as ColumnPath + '/../'.

    See also <a href="cassandra.html#Struct_ColumnPath">ColumnPath</a>
 */
struct ColumnParent {
    3: required string column_family,
    4: optional binary super_column,
}

/** The ColumnPath is the path to a single column in Cassandra. It might make sense to think of ColumnPath and
 * ColumnParent in terms of a directory structure.
 *
 * ColumnPath is used to looking up a single column.
 *
 * @param column_family. The name of the CF of the column being looked up.
 * @param super_column. The super column name.
 * @param column. The column name.
 */
struct ColumnPath {
    3: required string column_family,
    4: optional binary super_column,
    5: optional binary column,
}

/**
    A slice range is a structure that stores basic range, ordering and limit information for a query that will return
    multiple columns. It could be thought of as Cassandra's version of LIMIT and ORDER BY

    @param start. The column name to start the slice with. This attribute is not required, though there is no default value,
                  and can be safely set to '', i.e., an empty byte array, to start with the first column name. Otherwise, it
                  must a valid value under the rules of the Comparator defined for the given ColumnFamily.
    @param finish. The column name to stop the slice at. This attribute is not required, though there is no default value,
                   and can be safely set to an empty byte array to not stop until 'count' results are seen. Otherwise, it
                   must also be a value value to the ColumnFamily Comparator.
    @param reversed. Whether the results should be ordered in reversed order. Similar to ORDER BY blah DESC in SQL.
    @param count. How many keys to return. Similar to LIMIT 100 in SQL. May be arbitrarily large, but Thrift will
                  materialize the whole result into memory before returning it to the client, so be aware that you may
                  be better served by iterating through slices by passing the last value of one call in as the 'start'
                  of the next instead of increasing 'count' arbitrarily large.
    @param bitmasks. A list of OR-ed binary AND masks applied to the result set.
 */
struct SliceRange {
    1: required binary start,
    2: required binary finish,
    3: required bool reversed=0,
    4: required i32 count=100,
    5: optional list<binary> bitmasks,
}

/**
    A SlicePredicate is similar to a mathematic predicate (see http://en.wikipedia.org/wiki/Predicate_(mathematical_logic)),
    which is described as "a property that the elements of a set have in common."

    SlicePredicate's in Cassandra are described with either a list of column_names or a SliceRange.  If column_names is
    specified, slice_range is ignored.

    @param column_name. A list of column names to retrieve. This can be used similar to Memcached's "multi-get" feature
                        to fetch N known column names. For instance, if you know you wish to fetch columns 'Joe', 'Jack',
                        and 'Jim' you can pass those column names as a list to fetch all three at once.
    @param slice_range. A SliceRange describing how to range, order, and/or limit the slice.
 */
struct SlicePredicate {
    1: optional list<binary> column_names,
    2: optional SliceRange   slice_range,
}

/**
The semantics of start keys and tokens are slightly different.
Keys are start-inclusive; tokens are start-exclusive.  Token
ranges may also wrap -- that is, the end token may be less
than the start one.  Thus, a range from keyX to keyX is a
one-element range, but a range from tokenY to tokenY is the
full ring.
*/
struct KeyRange {
    1: optional binary start_key,
    2: optional binary end_key,
    3: optional string start_token,
    4: optional string end_token,
    5: required i32 count=100
}

/**
    A KeySlice is key followed by the data it maps to. A collection of KeySlice is returned by the get_range_slice operation.

    @param key. a row key
    @param columns. List of data represented by the key. Typically, the list is pared down to only the columns specified by
                    a SlicePredicate.
 */
struct KeySlice {
    1: required binary key,
    2: required list<ColumnOrSuperColumn> columns,
}

struct Deletion {
    1: required i64 timestamp,
    2: optional binary super_column,
    3: optional SlicePredicate predicate,
}

/**
    A Mutation is either an insert, represented by filling column_or_supercolumn, or a deletion, represented by filling the deletion attribute.
    @param column_or_supercolumn. An insert to a column or supercolumn
    @param deletion. A deletion of a column or supercolumn
*/
struct Mutation {
    1: optional ColumnOrSuperColumn column_or_supercolumn,
    2: optional Deletion deletion,
}

struct TokenRange {
    1: required string start_token,
    2: required string end_token,
    3: required list<string> endpoints,
}

/** The AccessLevel is an enum that expresses the authorized access level granted to an API user:
 *
 *      NONE       No access permitted.
 *      READONLY   Only read access is allowed.
 *      READWRITE  Read and write access is allowed.
 *      FULL       Read, write, and remove access is allowed.
*/
enum AccessLevel {
    NONE = 0,
    READONLY = 16,
    READWRITE = 32,
    FULL = 64,
}

/**
    Authentication requests can contain any data, dependent on the AuthenticationBackend used
*/
struct AuthenticationRequest {
    1: required map<string, string> credentials
}

/* describes a column family. */
struct CfDef {
    1: required string table,
    2: required string name,
    3: optional string column_type="Standard",
    4: optional string comparator_type="BytesType",
    5: optional string subcomparator_type="",
    6: optional string comment="",
    7: optional double row_cache_size=0,
    8: optional bool preload_row_cache=0,
    9: optional double key_cache_size=200000,
}

/* describes a keyspace. */
struct KsDef {
    1: required string name,
    2: required string strategy_class,
    3: required i32 replication_factor,
    5: required list<CfDef> cf_defs,    
}

service Cassandra {
  # auth methods
  AccessLevel login(1: required AuthenticationRequest auth_request) throws (1:AuthenticationException authnx, 2:AuthorizationException authzx),
 
  # set keyspace
  void set_keyspace(1: required string keyspace) throws (1:InvalidRequestException ire),
  
  # retrieval methods

  /**
    Get the Column or SuperColumn at the given column_path. If no value is present, NotFoundException is thrown. (This is
    the only method that can throw an exception under non-failure conditions.)
   */
  ColumnOrSuperColumn get(1:required binary key,
                          2:required ColumnPath column_path,
                          3:required ConsistencyLevel consistency_level=ONE)
                      throws (1:InvalidRequestException ire, 2:NotFoundException nfe, 3:UnavailableException ue, 4:TimedOutException te),

  /**
    Get the group of columns contained by column_parent (either a ColumnFamily name or a ColumnFamily/SuperColumn name
    pair) specified by the given SlicePredicate. If no matching values are found, an empty list is returned.
   */
  list<ColumnOrSuperColumn> get_slice(1:required binary key, 
                                      2:required ColumnParent column_parent, 
                                      3:required SlicePredicate predicate, 
                                      4:required ConsistencyLevel consistency_level=ONE)
                            throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Performs a get_slice for column_parent and predicate for the given keys in parallel.
  */
  map<binary,list<ColumnOrSuperColumn>> multiget_slice(1:required list<binary> keys, 
                                                       2:required ColumnParent column_parent, 
                                                       3:required SlicePredicate predicate, 
                                                       4:required ConsistencyLevel consistency_level=ONE)
                                        throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    returns the number of columns matching <code>predicate</code> for a particular <code>key</code>, 
    <code>ColumnFamily</code> and optionally <code>SuperColumn</code>.
  */
  i32 get_count(1:required binary key, 
                2:required ColumnParent column_parent, 
                3:required SlicePredicate predicate,
                4:required ConsistencyLevel consistency_level=ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Perform a get_count in parallel on the given list<binary> keys. The return value maps keys to the count found.
  */
  map<binary, i32> multiget_count(1:required string keyspace,
                2:required list<binary> keys,
                3:required ColumnParent column_parent,
                4:required SlicePredicate predicate,
                5:required ConsistencyLevel consistency_level=ONE)
      throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
   returns a subset of columns for a range of keys.
  */
  list<KeySlice> get_range_slices(1:required ColumnParent column_parent, 
                                  2:required SlicePredicate predicate,
                                  3:required KeyRange range,
                                  4:required ConsistencyLevel consistency_level=ONE)
                 throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  # modification methods

  /**
   * Insert a Column at the given column_parent.column_family and optional column_parent.super_column.
   */
  void insert(1:required binary key, 
              2:required ColumnParent column_parent,
              3:required Column column,
              4:required ConsistencyLevel consistency_level=ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Remove data from the row specified by key at the granularity specified by column_path, and the given timestamp. Note
    that all the values in column_path besides column_path.column_family are truly optional: you can remove the entire
    row by just specifying the ColumnFamily, or you can remove a SuperColumn or a single Column by specifying those levels too.
   */
  void remove(1:required binary key,
              2:required ColumnPath column_path,
              3:required i64 timestamp,
              4:ConsistencyLevel consistency_level=ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),

  /**
    Mutate many columns or super columns for many row keys. See also: Mutation.

    mutation_map maps key to column family to a list of Mutation objects to take place at that scope.
  **/
  void batch_mutate(1:required map<binary, map<string, list<Mutation>>> mutation_map,
                    2:required ConsistencyLevel consistency_level=ONE)
       throws (1:InvalidRequestException ire, 2:UnavailableException ue, 3:TimedOutException te),
       
  /**
   Truncate will mark and entire column family as deleted.
   From the user's perspective a successful call to truncate will result complete data deletion from cfname.
   Internally, however, disk space will not be immediatily released, as with all deletes in cassandra, this one
   only marks the data as deleted.
   The operation succeeds only if all hosts in the cluster at available and will throw an UnavailableException if 
   some hosts are down.
  */
  void truncate(1:required string keyspace,
                2:required string cfname)
       throws (1: InvalidRequestException ire, 2: UnavailableException ue),
    
  // Meta-APIs -- APIs to get information about the node or cluster,
  // rather than user data.  The nodeprobe program provides usage examples.

  /** list the defined keyspaces in this cluster */
  set<string> describe_keyspaces(),

  /** get the cluster name */
  string describe_cluster_name(),

  /** get the thrift api version */
  string describe_version(),

  /** get the token ring: a map of ranges to host addresses,
      represented as a set of TokenRange instead of a map from range
      to list of endpoints, because you can't use Thrift structs as
      map keys:
      https://issues.apache.org/jira/browse/THRIFT-162 

      for the same reason, we can't return a set here, even though
      order is neither important nor predictable. */
  list<TokenRange> describe_ring(1:required string keyspace),

  /** describe specified keyspace */
  map<string, map<string, string>> describe_keyspace(1:required string keyspace)
                                   throws (1:NotFoundException nfe),

  /** experimental API for hadoop/parallel query support.  
      may change violently and without warning. 

      returns list of token strings such that first subrange is (list[0], list[1]],
      next is (list[1], list[2]], etc. */
  list<string> describe_splits(1:required string start_token, 
  	                           2:required string end_token,
                               3:required i32 keys_per_split),
  
  void system_add_column_family(1:required CfDef cf_def)
    throws (1:InvalidRequestException ire),
    
  void system_drop_column_family(1:required string keyspace, 2:required string column_family)
    throws (1:InvalidRequestException ire), 
    
  void system_rename_column_family(1:required string keyspace, 2:required string old_name, 3:required string new_name)
    throws (1:InvalidRequestException ire),
  
  void system_add_keyspace(1:required KsDef ks_def)
    throws (1:InvalidRequestException ire),
  
  void system_drop_keyspace(1:required string keyspace)
    throws (1:InvalidRequestException ire),
    
  void system_rename_keyspace(1:required string old_name, 2:required string new_name)
    throws (1:InvalidRequestException ire),
  
}
