# Zipkin Hazelcast IMDG Server

When `zipkin.storage.type` == `hazelcast` is selected, storage
uses Hazelcast's In-Memory Data Grid (_IMDG_) as external storage.

See https://imdg.hazelcast.org or https://github.com/hazelcast

## `zipkin.storage.type` == `hazelcast`

`zipkin.storage.type` == `mem` stores trace data in the memory of the Zipkin
server. Capacity is limited by the process size of the Zipkin server, and
if this Zipkin server fails or is restarted the memory contents are lost.
Consequently this configuration isn't recommended for production.

`zipkin.storage.type` == `hazelcast` is as robust as you configure it to be.
Hazelcast is used for production data storage in many other projects.

Hazelcast IMDG is a cluster of processes that hold the trace data in
memory but external to the Zipkin server. Data access is still at in-memory
speeds rather than disk speeds, but capacity and robustness are added.

### Capacity

Capacity is managed by the number of IMDG server processes in the cluster.

If you start four IMDG server processes, you will have a certain amount
of storage. If you start four more, Hazelcast will automatically rebalance
any data held in the original four across all eight, and now overall
capacity has doubled.

As an in-memory data grid, data is held in memory. The capacity of each
process is controlled by how much memory the process is given. The
capacity of the grid as a whole is just the sum of each process's
capacity. All storage processes should be the same size.

Trace data is not automatically discarded to manage capacity, unless data
eviction or expiration are configured.

### Robustness

Data is mirrored across the processes in the cluster to give data safety.

The default mirroring is used here, meaning each data record has a master
copy on one IMDG server and one backup copy on another IMDG server. This
allows any one server to fail without data loss. If a server is lost one
of the data copies is lost and the other remains ; the remaining copy is
used to immediately rebuild the lost copy on any other server and the
configured level of mirroring is regained.

If losing more servers is a concern, increase the backup count accordingly.

## Hazelcast IMDG Server

This command starts one Hazelcast IMDG server:

```
java -jar zipkin-hazelcast-imdg-server/target/zipkin-hazelcast-imdg-server-*.jar
```

Repeat this command to start more instances, they should discover each
other and form a cluster.

The configuration file `hazelcast.xml` specifies the hosts where the
cluster runs. Add your IP addresses if using multiple hosts.

## IMap

Storage uses Hazelcast's `com.hazelcast.core.IMap` class.

This can be used as a drop-in replacement for Java's `java.util.Map`
but the Hazelcast version also has useful extensions such as querying
and indexing capabilities.

## Extensions

Trace data could be expired, deleted automatically by Hazelcast when
unused for a time.

Trace data could be evicted, deleted automatically by Hazelcast when
the cluster starts to get full.

Trace data could be analysed. Trace records are stored as Java objects,
the cluster is Java, so Java processing would be easy.

Consider making trace data partition aware, meaning to host related traces
on the same server when there are several servers. This can speed up search
operations but also runs the risk of overriding distribution rules so breaking
even balance of data. 