## Getting started

This plugin lets you run OpenSearch nodes without forming a cluster, using etcd as a shared configuration store.
This allows you to run a distributed OpenSearch system without the need for Zen Discovery or cluster coordination.

### Install and launch etcd

See instructions at https://etcd.io/docs/v3.5/install/.

On my Mac, this meant `brew install etcd`. Then I just ran the `etcd` executable and left it running in a terminal tab.

On Ubuntu, I needed to run `sudo apt install etcd-server etcd-client` to get both the `etcd` and `etcdctl` commands. Installing the server automatically started the `etcd` process.

You should also get the `etcdctl` command line tool included. You can interact with the running local etcd instance as follows:

```bash
# Write value 'bar' to key 'foo'
% etcdctl put foo bar
OK

# Read the value from key 'foo'
% etcdctl get foo
foo
bar

# Get all keys whose first byte is between ' ' (the earliest printable character) and '~' (the last)
%  etcdctl get ' ' '~'
foo
bar

# Delete the entry for key 'foo'
% etcdctl del foo
1
```

### Usage: Create a three-node cluster with two primary shards and a coordinator

#### Run three OpenSearch nodes from this branch

In order to validate that we can form a working distributed system without a cluster, we will start three OpenSearch nodes
locally. The first will serve as a coordinator, while the other two will be data nodes.

```bash
# Clone the repo
% git clone https://github.com/opensearch-project/cluster-etcd.git

# Enter the cloned repo
% cd cluster-etcd

# Run with the cluster-etcd plugin loaded and launch three nodes.
# The plugin is automatically installed and etcd endpoint is configured in build.gradle.
% ./gradlew run -PnumNodes=3

# In another tab, check the local cluster state for each node

# In the example below, this will be the coordinator node. Note that the node name is integTest-0.
% curl 'http://localhost:9200/_cluster/state?local&pretty'

# In the example below, this will be the first data node. Note that the node name is integTest-1.
% curl 'http://localhost:9201/_cluster/state?local&pretty'

# In the example below, this will be the second data node. Note that the node name is integTest-2.
% curl 'http://localhost:9202/_cluster/state?local&pretty'
```

Within a few seconds, the running nodes will publish heartbeats to etcd. You can see these with:

```bash
etcdctl get --prefix ''
```

You should see a response similar to:

```json
/integTest/search-unit/integTest-0/actual-state
{"nodeName":"integTest-0","address":"127.0.0.1","memoryUsedMB":65422,"heapMaxMB":512,"diskTotalMB":948584,"cpuUsedPercent":5,
"ephemeralId":"OYidGJ1ITdC2mMrZEW0BNQ","memoryUsedPercent":100,"heartbeatIntervalMillis":5000,"heapUsedMB":212,
"memoryMaxMB":65536,"port":9300,"heapUsedPercent":41,"nodeRouting":{},"nodeId":"pv1sDMfZRxSTMiW5VrbN9w","diskAvailableMB":817097,
"timestamp":1757713637663}
/integTest/search-unit/integTest-1/actual-state
{"nodeName":"integTest-1","address":"127.0.0.1","memoryUsedMB":65151,"heapMaxMB":512,"diskTotalMB":948584,"cpuUsedPercent":3,
"ephemeralId":"JtRlWuB5RgmxlWIcGl5wQw","memoryUsedPercent":99,"heartbeatIntervalMillis":5000,"heapUsedMB":191,
"memoryMaxMB":65536,"port":9301,"heapUsedPercent":37,"nodeRouting":{},"nodeId":"72ZSKe7GSBq2ZRvwHYw9zw","diskAvailableMB":817099,
"timestamp":1757713634266}
/integTest/search-unit/integTest-2/actual-state
{"nodeName":"integTest-2","address":"127.0.0.1","memoryUsedMB":65151,"heapMaxMB":512,"diskTotalMB":948584,"cpuUsedPercent":3,
"ephemeralId":"gxsZ43uxTVS_hDMbZ8Ns_g","memoryUsedPercent":99,"heartbeatIntervalMillis":5000,"heapUsedMB":212,
"memoryMaxMB":65536,"port":9302,"heapUsedPercent":41,"nodeRouting":{},"nodeId":"zF5QLAUpQXqIwaNOD5vDKQ","diskAvailableMB":817099,
"timestamp":1757713635798}
```

#### Push some state to etcd to add shards to the data nodes

The cluster-etcd plugin uses a split metadata approach that separates index configuration into
two distinct etcd keys:

- **Settings**: `/{clusterName}/indices/{index}/settings` - Basic index configuration needed by all nodes
- **Mappings**: `/{clusterName}/indices/{index}/mappings` - Field definitions needed primarily by data nodes

```bash
# Write index settings and mappings separately
# Settings are needed by both data nodes and coordinators
% cat << EOF | etcdctl put /integTest/indices/myindex/settings
{
  "index": {
    "number_of_shards": "2",
    "number_of_replicas": "0"
  }
}
EOF

# Mappings are needed by data nodes only (flattened structure)
% cat << EOF | etcdctl put /integTest/indices/myindex/mappings
{
  "properties": {
    "title": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword",
          "ignore_above": 256
        }
      }
    }
  }
}
EOF

# Assign primary for shard 0 of myindex to the node listening on port 9201/9301
% etcdctl put /integTest/search-unit/integTest-1/goal-state '{"local_shards":{"myindex":{"0":"PRIMARY"}}}'

# Assign primary for shard 1 of myindex to the node listening on port 9202/9302
% etcdctl put /integTest/search-unit/integTest-2/goal-state '{"local_shards":{"myindex":{"1":"PRIMARY"}}}'

# Check all keys to see the new structure. Within a few seconds, the heartbeats from each node
# will contain the new shards.
% etcdctl get --prefix ''

# Check the local cluster state on each data node
% curl 'http://localhost:9201/_cluster/state?local&pretty'
% curl 'http://localhost:9202/_cluster/state?local&pretty'

# Write a document to each shard. Here we're relying on knowing which shard each doc will land on (from trial and error).
# Note that if you try sending each document to the other data node, it will fail, since the data nodes don't know about
# each other and don't know where to forward the documents.
% curl -X POST -H 'Content-Type: application/json' http://localhost:9201/myindex/_doc/3 -d '{"title":"Hello from shard 0"}'
% curl -X POST -H 'Content-Type: application/json' http://localhost:9202/myindex/_doc/1 -d '{"title":"Hello from shard 1"}'

# Search the document on shard 0
% curl 'http://localhost:9201/myindex/_search?pretty'

# Search the document on shard 1
% curl 'http://localhost:9202/myindex/_search?pretty'
```

#### Add a coordinator

```bash
# Tell the coordinator about the data nodes.
% cat << EOF | etcdctl put /integTest/search-unit/integTest-0/goal-state
{
  "remote_shards": {
    "indices": {
      "myindex": {
        "shard_routing" : [
          [
            {"node_name": "integTest-1", "primary": true }
          ],
          [
            {"node_name": "integTest-2", "primary": true }
          ]
        ]
      }
    }
  }
}
EOF

# Search via the coordinator node. You'll see both documents added above
% curl 'http://localhost:9200/myindex/_search?pretty'

# Index a batch of documents (surely hitting both shards) via the coordinator node
% curl -X POST -H 'Content-Type: application/json' http://localhost:9200/myindex/_bulk -d '
{ "index": {"_id":"2"}}
{"title": "Document 2"}
{ "index": {"_id":"4"}}
{"title": "Document 4"}
{ "index": {"_id":"5"}}
{"title": "Document 5"}
{ "index": {"_id":"6"}}
{"title": "Document 6"}
{ "index": {"_id":"7"}}
{"title": "Document 7"}
{ "index": {"_id":"8"}}
{"title": "Document 8"}
{ "index": {"_id":"9"}}
{"title": "Document 9"}
{ "index": {"_id":"10"}}
{"title": "Document 10"}
'

# Search via the coordinator node. You'll see 10 documents. If you search each data node you'll see around half.
% curl 'http://localhost:9200/myindex/_search?pretty'
```

### Usage: Set up remote store based segment replication

In this example, we will provision an index with a single primary shard and one search replica.
These will be placed on a pair of nodes running locally. The data will replicate via the remote store
from the primary to the search replica. For simplicity, we'll use the `fs` repository type, which
just uses a directory in the local filesystem.

Before following these steps, make sure that you have stopped your runnning OpenSearch instance (say,
if you ran the previous example). Ensure that etcd is still running, though.

#### Configure the index and start OpenSearch

```bash
# Create a temporary directory to hold the "remote store".
% REPO_DIR=$(mktemp -d -t remote_repo)

# Output the temporary directory so you can check its contents later.
% echo $REPO_DIR

# Push the index configuration to etcd.
% etcdctl put /integTest/indices/segrep-index/settings << EOF
{
  "index": {
    "number_of_shards": "1",
    "number_of_search_replicas": "1",
    "number_of_replicas": 0,
    "remote_store.enabled": true,
    "replication.type": "SEGMENT",
    "remote_store.segment.repository":"segrep-repo"
  }
}
EOF

% etcdctl put /integTest/indices/segrep-index/mappings << EOF
{
  "properties": {
    "title": {
      "type": "text"
    }
  }
}
EOF

# Configure the primary.
% etcdctl put /integTest/search-unit/integTest-0/goal-state '{"local_shards":{"segrep-index":{"0":"PRIMARY"}}}'

# Configure the search replica
% etcdctl put /integTest/search-unit/integTest-1/goal-state '{"local_shards":{"segrep-index":{"0":"SEARCH_REPLICA"}}}'

# Launch OpenSearch with remote store configuration
% ./gradlew run -PnumNodes=2 \
    -Dtests.opensearch.node.attr.remote_store.segment.repository=segrep-repo \
    -Dtests.opensearch.node.attr.remote_store.repository.my-repo-1.type=fs \
    -Dtests.opensearch.node.attr.remote_store.mode=segments_only \
    -Dtests.opensearch.node.attr.remote_store.repository.my-repo-1.settings.location="$REPO_DIR" \
    -Dtests.opensearch.path.repo="$REPO_DIR"
```

#### Verifying replication

In another window, you can verify that documents are replicated from the primary to the search replica.

```bash
# Add a document to the primary
% curl -X POST -H 'Content-Type: application/json' http://localhost:9200/myindex/_doc/1 -d '{"title":"Writing to the primary"}'

# Search for it from the search replica
% curl 'http://localhost:9201/myindex/_search?pretty'

# Check the contents of the "remote store"
# Note that you'll probably have to paste the value of $REPO_DIR from the previous window.
% find $REPO_DIR
```

### Usage: Set up primary/replica document replication

As with the previous example, make sure that you've stopped the running OpenSearch process before writing the new goal state.

In this example, we will launch two nodes with "traditional" document replication. This kind of goes against the premise of
"clusterless" OpenSearch, since it involves direct communication between data nodes. However, the only communication is
within each replication group. The primary node for a given shard must know where all replicas for that shard are located.
All replicas for a shard must know which node holds the primary for that shard.

#### Configure the index and start OpenSearch

```bash
# Write the index settings and mappings
% cat << EOF | etcdctl put /integTest/indices/docrep-index/settings
{
  "index": {
    "number_of_shards": "1",
    "number_of_replicas": "1"
  }
}
EOF

% cat << EOF | etcdctl put /integTest/indices/docrep-index/mappings
{
  "properties": {
    "title": {
      "type": "text"
    }
  }
}
EOF

# Create the primary shard on integTask-0, pointing to the replica on integTask-1
% etcdctl put /integTest/search-unit/integTest-0/goal-state \
  '{"local_shards":{"docrep-index":{"0":{"type":"primary","replica_nodes":["integTest-1"]}}}}'

# Create the replica shard on integTask-1, pointing to the primary on integTask-0
% etcdctl put /integTest/search-unit/integTest-1/goal-state \
  '{"local_shards":{"docrep-index":{"0":{"type":"replica","primary_node":"integTest-0"}}}}'

# Start OpenSearch with two nodes
% ./gradlew run -PnumNodes=2
```

#### Cluster state convergence

There is some coordination that needs to occur between the primary and replica before both shards move into a "STARTED" state.
Specifically, the primary shard must start before the replica can be assigned. Once the replica is assigned, the primary shard
must learn the replica's allocation ID, which it finds from the replica node's heartbeat. Then the replica is able to to
recover from the primary. Finally, the primary waits until the replica's heartbeat shows that it has fully started. Until the
replica is assigned, the OpenSearch process will log messages like:

```
[2025-09-15T16:56:13,956][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:14,357][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-1] Reloading node state for key: /integTest/search-unit/integTest-1/goal-state
[2025-09-15T16:56:14,974][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:15,375][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-1] Reloading node state for key: /integTest/search-unit/integTest-1/goal-state
[2025-09-15T16:56:15,991][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:16,391][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-1] Reloading node state for key: /integTest/search-unit/integTest-1/goal-state
[2025-09-15T16:56:17,003][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:17,405][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-1] Reloading node state for key: /integTest/search-unit/integTest-1/goal-state
```

Once the replica has been assigned, only the node holding the primary shard will keep polling, producing log messages like:

```
[2025-09-15T16:56:20,051][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:21,060][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:22,076][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:23,094][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
[2025-09-15T16:56:24,107][INFO ][o.o.c.e.ETCDWatcher      ] [integTest-0] Reloading node state for key: /integTest/search-unit/integTest-0/goal-state
```

Once the state has converged, you can verify that updates are replicated from the primary to the replica, just like with
remote store segment replication.

#### Verifying replication

```
# Add a document to the primary
% curl -X POST -H 'Content-Type: application/json' http://localhost:9200/myindex/_doc/1 -d '{"title":"Writing to the primary"}'

# Search for it from the search replica
% curl 'http://localhost:9201/myindex/_search?pretty'
```


### Set up full integration test with etcd and controller in docker locally

Start local environment
```
# Add -PuseDocker to create full integration test local environment, use -Pgit-hash to specify controler branch
# can we use main branch after https://github.com/TejasNaikk/cluster-controller/pull/28 merged
% ./gradlew run -PnumNodes=3 -PuseDocker -Pgit-hash=dc/schedule-tasks 
```

Add initial config key for the controller
```
# Register a cluster name for controller to start the managing the test cluster
% docker exec etcd-local etcdctl put '/multi-cluster/clusters/integTest/metadata' '{"cluster":"integTest"}'
```

Create a test index via controller API

```
% curl -X PUT "http://localhost:8080/integTest/test-index-1" -H "Content-Type: application/json"  -d '{ "settings": { "number_of_shards": 1, "number_of_replicas": 1 }, "mappings": { "properties": { "custom_field": {"type": "keyword"} } } }'
```

Check controller logs
```
% docker logs -f clusteretcdtest-controller-1 
```

Optionally start a etcd-workbench to monitor the etcd keys
```
% docker run --name my-etcd-workbench -p 8002:8002 -d tzfun/etcd-workbench:latest
```

Clean up the environment
```
# stop all the containers and remove images
% ./gradlew composeDown

# To ensure next run can picked updated controler code, we can clear the docker cache before running
% docker builder prune --all

```



## Code of Conduct

The project's [Code of Conduct](CODE_OF_CONDUCT.md) outlines our expectations for all participants in our community, based on the [OpenSearch Code of Conduct](https://opensearch.org/code-of-conduct/). Please contact [conduct@opensearch.foundation](mailto:conduct@opensearch.foundation) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.
