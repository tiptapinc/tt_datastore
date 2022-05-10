## v0.0.20
* update to work with couchbase version >= 4.0.0

## v0.0.19
* use couchbase python SDK v3+

## v0.0.18
* updated directory structure

## v0.0.17
* add n1ql_index_list method
* add n1ql_index_create method

## v0.0.16
* add n1ql_query method

## v0.0.15
* allow connection kwargs when creating CouchbaseDatastore
		- operation_timeout
		- config_total_timeout
		- config_node_timeout

## v0.0.14
* remove NotFoundError again for couchbase 3.0

## v0.0.13
* switch to couchbase version that supports Role-Based Access Control (RBAC)
* requires couchbase v2.2.6 or higher
* not backward compatible with previous tt_datastore

## v0.0.12
* added support for python 3

## v0.0.11
* lock required couchbase version

## v0.0.10
* add update_with_cas and set_with_cas methods

## v0.0.9
* restore NotFoundError

## v0.0.8
* added lock and unlock methods

## v0.0.7:
* set 'quiet' to True for Bucket
* remove exception handling
* add **kwargs to create, read, update, set, delete, get_multi methods
* add read_with_cas method

## v0.0.6:
* use couchbase.bucket instead of deprecated couchbase.connection
* add get_multi, design_get, and design_create methods

## v0.0.5:

* added ability to set global json encoder when creating
  a Couchbase datastore

## v0.0.4:

* removed "account" and "accountPassword" arguments
* added "set" operation
* eliminated unneccessary "try-except" clauses
* added traceback to "except" clauses

## v0.0.3:

* cosmetic cleanups suggested by PEP-8 linter
* fixed views

## v0.0.2:

* removed call to logging.basicConfig()


## v0.0.1:

* Initial release
