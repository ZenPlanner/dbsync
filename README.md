db-sync
=======

Synchronize two JDBC databases


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
* Re-enable constraints when finished
* Sync everything from non-partitionId-having tables
* Delete
* Update
* Swing UI
* Filter on any number and type of fields
* Store settings in config file

In progress
-----------

TODO
----
* Only re-enable constraints that were originally enabled
* Add obfuscation
* Per row progress indicator based on select count(*)
* Filters with "or", not just "and"
* Find and tune performance bottlenecks
* Multi-threading?
* Refactor and test

Bugs
----
No known bugs at this time :)