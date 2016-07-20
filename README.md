sql-raft
========
sql-raft is a tool to help manage consensus for SQL databases with a master and n-replica setup specifically PostgreSQL and MySQL.

In a master-slave database setup consensus needs be established amongst the cluster members to determine which node should be the master when members join and leave the cluster.  Once this is determined configuration changes need to be made on each database.  

This is where sql-raft comes to play.  It keeps track of when a node has changed state and calls the registered handler.  It provides orchestration between database nodes to appropriately promote and demote themselves based on cluster changes (i.e. nodes joining and leaving the cluster).

# Supported Databases
Though sql-raft is designed to support any databse, currently only the following database drivers are included:

- PostgreSQL > 9.5

# Roadmap

- Support for PostgreSQL 9.3 and up
- Support for MySQL