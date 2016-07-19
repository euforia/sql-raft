FROM postgres:9.5

ADD ./sql-raft /

ENTRYPOINT ["/sql-raft"]
