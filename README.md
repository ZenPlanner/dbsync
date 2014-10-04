db-sync
=======

Syncronize two JDBC databases


Done
----
* Enable identity insert
* Disable constraints
* Get schema from DB
* Find only tables with primary key and partitionId columns
* Execute hash-query against source and destination DBs
* Detect row state as inserted, updated, deleted, or unchanged
* Get the actual row data for changed rows
* Bulk insert updated records

TODO
----
* Re-enable constraints when finished
* Update
* Delete
* Sync everything from non-partitionId-having tables
* Swing UI
* Store settings in config file
* Performance testing
* Refactor and test
