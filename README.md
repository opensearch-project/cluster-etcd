## Running OpenSearch with etcd cluster state

Within this branch, I'm updating the README to explain how to get started with etcd.

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

### Run three OpenSearch nodes from this branch

In order to validate that we can form a working distributed system without a cluster, we will start three OpenSearch nodes
locally. The first will serve as a coordinator, while the other two will be data nodes

```bash
# Clone the repo
% git clone https://github.com/msfroh/OpenSearch.git

# Enter the cloned repo
% cd OpenSearch

# Checkout the correct branch
% git checkout clusterless_datanode

# Run with the cluster-etcd plugin loaded and launch three nodes. We also need to set the clusterless mode feature flag.
% ./gradlew run -PinstalledPlugins="['cluster-etcd']" -PnumNodes=3 -Dtests.opensearch.opensearch.experimental.feature.clusterless.enabled=true

# In another tab, check the local cluster state for each node

# In the examples below, this will be the coordinator node. Note that the node name is runTask-0.
% curl 'http://localhost:9200/_cluster/state?local&pretty'

# In the examples below, this will be the first data node. Note that the node name is runTask-1.
% curl 'http://localhost:9201/_cluster/state?local&pretty'

# In the examples below, this will be the second data node. Note that the node name is runTask-2.
% curl 'http://localhost:9202/_cluster/state?local&pretty'
```

### Push some state to etcd to start the data nodes

The cluster-etcd plugin now uses a split metadata approach that separates index configuration into distinct etcd keys:

- **Settings**: `/indices/{index}/settings` - Basic index configuration needed by all nodes
- **Mappings**: `/indices/{index}/mappings` - Field definitions needed primarily by data nodes  

This approach reduces etcd storage requirements and simplifies control plane logic by filtering out data plane implementation details.

#### Automatic UUID and Version Generation

The plugin now automatically generates index metadata constants programmatically rather than storing them in etcd:

- **UUID**: Generated deterministically from the index name using UUID.nameUUIDFromBytes(), ensuring all nodes produce identical UUIDs for the same index
- **Version**: Set to the current OpenSearch version (Version.CURRENT)
- **Creation Date**: Generated deterministically from the index name to ensure consistency across nodes

This eliminates the need to manually specify these values in etcd and ensures all nodes maintain identical metadata, preventing cluster coordination failures due to hashcode mismatches.

```bash
# Write index settings and mappings separately (new split metadata approach)
# Settings are needed by both data nodes and coordinators
% cat << EOF | etcdctl put runTask/indices/myindex/settings
{
  "index": {
    "number_of_shards": "1",
    "number_of_replicas": "0"
  }
}
EOF

# Mappings are needed by data nodes only (flattened structure)
% cat << EOF | etcdctl put runTask/indices/myindex/mappings
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
% etcdctl put runTask/search-unit/runTask-1/goal-state '{"local_shards":{"myindex":{"0":"PRIMARY"}}}'

# Assign primary for shard 1 of myindex to the node listening on port 9202/9302
% etcdctl put runTask/search-unit/runTask-2/goal-state '{"local_shards":{"myindex":{"1":"PRIMARY"}}}'

# Verify the split metadata was stored correctly
% etcdctl get "runTask/indices/myindex/settings"
% etcdctl get "runTask/indices/myindex/mappings"

# Check all keys to see the new structure
% etcdctl get "" --from-key --keys-only

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

### Add a coordinator

The coordinator automatically resolves node names to node IDs by reading health data, eliminating the need to manually extract node IDs and ephemeral IDs.

```bash
# Tell the coordinator about the data nodes using their node names (not IDs).
# Note: Index UUIDs are now generated automatically from the index name, so no need to specify them
% cat << EOF | etcdctl put runTask/search-unit/runTask-0/goal-state
{
  "remote_shards": {
    "indices": {
      "myindex": {
        "shard_routing" : [
          [
            {"node_name": "runTask-1", "primary": true }
          ],
          [
            {"node_name": "runTask-2", "primary": true }
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

### Heartbeat and Health Data

Nodes automatically publish health and status information to ETCD at the path `{cluster_name}/search-unit/{node_name}/actual-state`.

```bash
# View heartbeat data for all nodes
% etcdctl get "runTask/search-unit/" --prefix

# View specific node's health data  
% etcdctl get "runTask/search-unit/<nodename>/actual-state"
```

<img src="https://opensearch.org/assets/img/opensearch-logo-themed.svg" height="64px">

[![Chat](https://img.shields.io/badge/chat-on%20forums-blue)](https://forum.opensearch.org/c/opensearch/)
[![Documentation](https://img.shields.io/badge/documentation-reference-blue)](https://opensearch.org/docs/latest/opensearch/index/)
[![Code Coverage](https://codecov.io/gh/opensearch-project/OpenSearch/branch/main/graph/badge.svg)](https://codecov.io/gh/opensearch-project/OpenSearch)
[![Untriaged Issues](https://img.shields.io/github/issues/opensearch-project/OpenSearch/untriaged?labelColor=red)](https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+label%3A"untriaged")
[![Security Vulnerabilities](https://img.shields.io/github/issues/opensearch-project/OpenSearch/security%20vulnerability?labelColor=red)](https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+label%3A"security%20vulnerability")
[![Open Issues](https://img.shields.io/github/issues/opensearch-project/OpenSearch)](https://github.com/opensearch-project/OpenSearch/issues)
[![Open Pull Requests](https://img.shields.io/github/issues-pr/opensearch-project/OpenSearch)](https://github.com/opensearch-project/OpenSearch/pulls)
[![2.19.3 Open Issues](https://img.shields.io/github/issues/opensearch-project/OpenSearch/v2.19.3)](https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+label%3A"v2.19.3")
[![2.18.1 Open Issues](https://img.shields.io/github/issues/opensearch-project/OpenSearch/v2.18.1)](https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+label%3A"v2.18.1")
[![3.0.0 Open Issues](https://img.shields.io/github/issues/opensearch-project/OpenSearch/v3.0.0)](https://github.com/opensearch-project/OpenSearch/issues?q=is%3Aissue+is%3Aopen+label%3A"v3.0.0")
[![GHA gradle check](https://github.com/opensearch-project/OpenSearch/actions/workflows/gradle-check.yml/badge.svg)](https://github.com/opensearch-project/OpenSearch/actions/workflows/gradle-check.yml)
[![GHA validate pull request](https://github.com/opensearch-project/OpenSearch/actions/workflows/wrapper.yml/badge.svg)](https://github.com/opensearch-project/OpenSearch/actions/workflows/wrapper.yml)
[![GHA precommit](https://github.com/opensearch-project/OpenSearch/actions/workflows/precommit.yml/badge.svg)](https://github.com/opensearch-project/OpenSearch/actions/workflows/precommit.yml)
[![Jenkins gradle check job](https://img.shields.io/jenkins/build?jobUrl=https%3A%2F%2Fbuild.ci.opensearch.org%2Fjob%2Fgradle-check%2F&label=Jenkins%20Gradle%20Check)](https://build.ci.opensearch.org/job/gradle-check/)

- [Welcome!](#welcome)
- [Project Resources](#project-resources)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)
- [Trademark](#trademark)

## Welcome!

**OpenSearch** is [a community-driven, open source fork](https://aws.amazon.com/blogs/opensource/introducing-opensearch/) of [Elasticsearch](https://en.wikipedia.org/wiki/Elasticsearch) and [Kibana](https://en.wikipedia.org/wiki/Kibana) following the [license change](https://blog.opensource.org/the-sspl-is-not-an-open-source-license/) in early 2021. We're looking to sustain (and evolve!) a search and analytics suite for the multitude of businesses who are dependent on the rights granted by the original, [Apache v2.0 License](LICENSE.txt).

## Project Resources

* [Project Website](https://opensearch.org/)
* [Downloads](https://opensearch.org/downloads.html)
* [Documentation](https://opensearch.org/docs/)
* Need help? Try [Forums](https://discuss.opendistrocommunity.dev/)
* [Project Principles](https://opensearch.org/#principles)
* [Contributing to OpenSearch](CONTRIBUTING.md)
* [Maintainer Responsibilities](MAINTAINERS.md)
* [Release Management](RELEASING.md)
* [Admin Responsibilities](ADMINS.md)
* [Testing](TESTING.md)
* [Security](SECURITY.md)

## Code of Conduct

The project's [Code of Conduct](CODE_OF_CONDUCT.md) outlines our expectations for all participants in our community, based on the [OpenSearch Code of Conduct](https://opensearch.org/code-of-conduct/). Please contact [conduct@opensearch.foundation](mailto:conduct@opensearch.foundation) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify OpenSearch Security directly via email to security@opensearch.org. Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](LICENSE.txt).

## Copyright

Copyright OpenSearch Contributors. See [NOTICE](NOTICE.txt) for details.

## Trademark

OpenSearch is a registered trademark of Amazon Web Services.

OpenSearch includes certain Apache-licensed Elasticsearch code from Elasticsearch B.V. and other source code. Elasticsearch B.V. is not the source of that other source code. ELASTICSEARCH is a registered trademark of Elasticsearch B.V.
