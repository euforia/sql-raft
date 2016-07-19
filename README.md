sql-raft
========
sql-raft is a tool help manage consensus pertaining to SQL databases specifically PostgreSQL and MySQL.

In order to effective promote and demote masters and slaves consensus needs be established amongst the cluster members and appropriate apply changes to the database.  This is where sql-raft comes to play.  It allows to orchestrate between database nodes to appropriately promote and demote themselves based on cluster changes i.e nodes joining and leaving the cluster.
