# Running the whole system on Kubernetes

**Status: working on `kind` (Colima) and on Apple `container` 1.7.1.2.** One command brings up everything
[`DEMO.md`](../DEMO.md) runs by hand, on a local Kubernetes cluster, and `bin/e2e.sh` then does what `DEMO.md` does and
kills a data node and a supervisor. It passes on both runtimes, against the file ingestion source and against Kafka.

| piece | in the cluster |
|---|---|
| store | etcd, 3 members (StatefulSet, a volume each) |
| control plane | `control-plane/server`, 2 replicas, `SingleTenantResolver` (no authentication) |
| supervisor | `supervisor/supervisor-app`, 2 replicas with `--write`, and a Job that defines pool `shared-a` |
| data nodes | OpenSearch 3.10.0-SNAPSHOT with the clusterless plugin, 5 pods (StatefulSet, a volume each), all `data,coordinator` |
| segment store | an `fs` repository on a directory shared by every data pod (see [Limits](#limits)) |
| ingestion | the file source, or with `--kafka` a single-broker Kafka |

## Quick start

You need a Java 21 toolchain, `kubectl`, `curl` and `python3`, an OpenSearch checkout (below), and one of two ways to
run a cluster. The scripts read `CLUSTER_RUNTIME` (default `kind`); everything above that layer is identical.

### Option A: `kind` on a Docker daemon (Colima)

```bash
brew install colima docker kind kubectl
colima start -p clusterless --cpu 8 --memory 16
docker context use colima-clusterless
```

Any Docker daemon works. `docker` must be able to build and `kind` to reach it. Without buildx, Docker falls back to
its legacy builder, which the Dockerfiles here are written to tolerate.

### Option B: Apple `container`

```bash
container system start
export CLUSTER_RUNTIME=container          # CLUSTER_CPUS=8 and CLUSTER_MEMORY=16G are the defaults
```

`up.sh` then runs `container cluster create --name clusterless --cpus 8 --memory 16G`, writes the cluster's context
to `deploy/build/kubeconfig-clusterless` and uses it, with `--context clusterless`, for every `kubectl`.
(`cluster create` also adds the context to `~/.kube/config`.)
Images are built with `container build` and loaded the way `kind load` does it, by `container image save` and
`ctr images import` in the node (in 1.7.1.2 `--shared-image-store` still sent the kubelet to the registry for an
image built on the host). They use fully qualified `docker.io/clusterless/...` names, because `container` prefixes an unqualified name with its own default registry
and a pod would then not find it.

`container cluster` installs no StorageClass, so `up.sh` applies the local-path provisioner the node image carries
(`/kind/manifests/default-storage.yaml`, what `kind` applies itself). `container cluster` is marked EXPERIMENTAL in the CLI. It takes no volume or port options, which is why nothing in
this deployment needs either from the host. To use `kubectl` by hand:
`kubectl --kubeconfig deploy/build/kubeconfig-clusterless --context clusterless -n clusterless get pods`.

### Build, run, test

The data node needs an OpenSearch distribution built from the same version as the plugin, which exists only as a
local build. In an OpenSearch checkout whose `buildSrc/version.properties` says `3.10.0` (main has moved on to 3.11;
use the last 3.10 commit), with Java 21:

```bash
./gradlew publishToMavenLocal :distribution:archives:linux-arm64-tar:assemble \
    :plugins:repository-s3:assemble :plugins:ingestion-kafka:assemble
```

(`linux-tar` rather than `linux-arm64-tar` on an x86 machine.) Then, from this repository:

```bash
deploy/bin/build-images.sh /path/to/OpenSearch   # stages build contexts with Gradle, builds three images
deploy/bin/up.sh                                 # cluster, manifests, capabilities; add --kafka for a broker
deploy/bin/e2e.sh                                # add --kafka if up.sh was given it
deploy/bin/down.sh                               # deletes the cluster and every volume in it
```

`up.sh` is safe to re-run. After rebuilding an image under the same `:dev` tag, run `up.sh --restart` so running
pods pick it up. If `down.sh` needs to run against the other runtime, set `CLUSTER_RUNTIME` the same way.

`e2e.sh` reaches the control plane through a `kubectl port-forward` that it starts and stops itself, on
`localhost:9250` (`CONTROL_PLANE_PORT` changes it). To use the control plane by hand, run the same forward:

```bash
kubectl --context kind-clusterless -n clusterless port-forward svc/control-plane 9250:9250
curl localhost:9250/_cat/shards?v
```

Five OpenSearch nodes with 512 MB heaps, etcd and the rest need about 12 GiB; 16 GiB is the tested size.

## What `e2e.sh` checks

1. Creates an index through the control plane: 5 shards, one search replica each, segment replication.
2. The supervisor places all 10 copies, and every one starts.
3. Writes 2 documents per shard through the ingestion source. A coordinator counts and searches all 10, and every
   copy of every shard holds its 2 documents, read per copy from each node's shard stats. A search replica can only
   have them by reading the segment store.
4. Kills the pod holding shard 0's primary. It returns under the same node id (its volume persisted), every copy
   starts again, and ingestion resumes.
5. Kills the supervisor holding the pool's lease. The other takes it over and places a second index.

It stops at the first failure, and on a timeout in step 3 prints every copy's document count. It deletes its indices
on success; `KEEP=1` keeps them.

## What it does not check

- **A node that loses its disk.** Step 4 restarts a pod on its volume. A restart that wipes the disk is a different
  test (`data-plane/MILESTONES.md`, M7.1).
- **S3.** `repository-s3` is installed in the image and unused. See [Limits](#limits).
- **More than one machine.** Every pod runs on one cluster node, so addresses between machines, per-node disks and
  network faults are not exercised.
- **Anything about availability of etcd or Kafka.** Losing an etcd member or the Kafka broker is not tested; Kafka
  runs one broker with replication factor 1.

## How the hard parts are handled

**The data node image.** `bin/build-images.sh` runs `./gradlew -p deploy stage`, a build of its own (the root build
neither stages images nor needs an OpenSearch distribution). It assembles one build context per image under
`deploy/build/images/`. The data node's Dockerfile extracts the distribution, installs four plugins with
`opensearch-plugin install` (the data plane, `ingestion-fs` from the data plane's `ingestionFsBundle`,
`repository-s3` and `ingestion-kafka`), and drops the etcd backend and its client libraries **into the data plane
plugin's directory**, not the node's `lib/`. The backend jars are filtered at staging by what the server already
carries, and `check-jar-hell.sh` then compares classes against the node's `lib/` during the image build, so a
duplicate fails the build and not every pod. It once found `javax.annotation-api` against the node's
`jakarta.annotation-api`: one library under two names.

**Node settings** live in `k8s/base/data-node.yml`, one file for every node, which is what keeps the remote store
repository identical everywhere. The ones that matter:

- `clusterless.store: etcd://etcd:2379/opensearch`, `clusterless.node.pool: shared-a`.
- `clusterless.apply: true`. It defaults to false, which only logs what acting would do, and a node left there
  looks healthy and does nothing.
- `clusterless.node.roles: data,coordinator`, so a search through any node reaches every shard.
- `network.publish_host: ${POD_IP}`, from the downward API, because the advertised address is what the supervisor
  projects and peers connect to; `127.0.0.1` reaches nothing.
- `discovery.seed_hosts: []`. A clusterless node discovers nothing, but binding a non-loopback address enforces the
  bootstrap checks, and the discovery check fails a node with no discovery setting at all.
- `node.attr.remote_store.mode: segments_only` and one repository named `segments`.

**Node identity.** The data nodes are a StatefulSet with a volume per pod, so a restarted pod keeps its data
directory and its node id; the supervisor places by node id.

**Capabilities.** `up.sh` generates the control plane's `--capabilities` file from the five running nodes
(`control-plane/tools/dump-capabilities.sh`) and stores it as a ConfigMap, because it carries the minimum node
version new indices are stamped with: a stale minimum is safe and a too-high one is not. The control plane reads it
at startup, so the control plane pods wait for the ConfigMap. `_cluster/settings_registry` depends on a proposed
upstream change; without it `up.sh` writes an empty registry and the control plane accepts any setting.

**The pool.** A Job runs `--define-pool=shared-a` once. That command exits 1 both when the pool is already defined and
when the store is unreachable, so the Job also reads its log to tell them apart.

**Directories in the cluster node.** The segment store and the file source's directory are `hostPath` volumes in the
cluster node's own filesystem, shared by every pod, kept across pod restarts and gone with the cluster. A `prepare`
init container (privileged, root) raises `vm.max_map_count` through `/proc` and hands both directories to uid 1000.

**Kafka.** `k8s/kafka` is an overlay on `k8s/base` that adds one KRaft broker. A shard reads the partition with its
own number, so `e2e.sh --kafka` creates a topic with one partition per shard and produces round-robin from a fresh
producer, putting the same number of messages on each. The index sets `pointer.init.reset` to `earliest`; the default
is `latest`, which skips documents produced before a shard starts. Messages are the same
`{"_id", "_op_type", "_source"}` JSON the file source reads.

## Limits

- **The segment store is an `fs` repository, not S3.** It works because everything runs on one cluster node;
  across machines the segment store has to be a shared S3-compatible service, and a pod-local store cannot work
  because a search replica reads the segments its primary wrote. The settings change is in `data-node.yml`
  (`type: s3`, `bucket`, `client`, and the `s3.client.*` endpoint settings, with credentials in the keystore).
  `data-node/entrypoint.sh` loads `S3_ACCESS_KEY` and `S3_SECRET_KEY` into it when they are set.
- **MinIO's community images are no longer pullable** (Docker Hub says the repository does not exist; quay.io
  answers 401), so no S3 server is included. The AWS emulator's Docker image is built from a downloaded RPM and is
  HTTPS-only with a certificate and credentials regenerated at every start, held in memory only. It could sit behind a
  plain-HTTP proxy in its pod and give the e2e test an S3 path; that is not built.
- **Single cluster node.** `container cluster create --workers` would break the shared `hostPath` directories.
- **One etcd quorum and one Kafka broker**, on volumes that live and die with the cluster.

## Things that will bite

- **The control plane has no authentication.** The tenant comes from the `x-tenant-id` header; `SingleTenantResolver`
  serves everything as `default`. Its Service is ClusterIP only. Keep any port-forward on loopback.
- **A control plane that comes up before any supervisor answers `503`** from `cluster.health`, `cluster.state` and the
  cat tables for 30 seconds, because `status/cluster` is the supervisor's heartbeat. That is correct and looks like a
  bug; `e2e.sh` waits for it.
- **A supervisor is read-only unless told `--write`** (`SUPERVISOR_WRITE=true` here). Forgetting it observes everything
  and acts on nothing.
- **Two supervisors, one pool.** Only the holder of `supervisor/pools/<pool>/lease` acts; the other holds no pool, which is
  the healthy state. Read the lease, or `supervisor/status/members/<id>`, not either one's log.
- **The control plane takes `--port 9250`, not `--port=9250`.** Its option parser has no `=` form and exits 2 on one.
  The supervisor's does, and also reads `SUPERVISOR_<OPTION>`.
- **Version skew is deliberate.** The control plane claims OpenSearch 3.9.0 while the nodes are 3.10.0-SNAPSHOT.
- **Changing a node setting re-rolls the pods.** The ConfigMap is generated, so its name carries a hash of the file.

## Debugging

```bash
kubectl -n clusterless get pods                       # add --context kind-clusterless, or --kubeconfig for container
kubectl -n clusterless logs data-0 --tail=60
kubectl -n clusterless exec data-0 -- curl -s localhost:9200/_clusterless/status      # what a node read, applied, wrote
kubectl -n clusterless exec etcd-0 -- etcdctl get --print-value-only /opensearch/supervisor/pools/shared-a/lease
kubectl -n clusterless exec etcd-0 -- etcdctl get --prefix --keys-only /opensearch/ | head
```

A node's `assignment.deferred` in `_clusterless/status` lists copies it was told to hold and declined, with a reason:
the answer when the supervisor says a shard is unassigned and the node looks healthy.

## Where to read

| for | read |
|---|---|
| the manual steps this automates | [`DEMO.md`](../DEMO.md) |
| node settings, the remote store, ingestion | [`data-plane/README.md`](../data-plane/README.md), `data-plane/plugin/build.gradle` |
| what a node needs to run and why | [`data-plane/CLAUDE.md`](../data-plane/CLAUDE.md), [`data-plane/WORKING_NOTES.md`](../data-plane/WORKING_NOTES.md) |
| the supervisor's options, pools and leases | [`supervisor/README.md`](../supervisor/README.md) (`--help` lists every flag) |
| the control plane's flags, tenancy and capabilities | [`control-plane/README.md`](../control-plane/README.md) |
| store backends and their limits | [`kv-store/README.md`](../kv-store/README.md), [`kv-store/BACKENDS.md`](../kv-store/BACKENDS.md) |
| what each component has and has not proven | each component's `MILESTONES.md` or `README.md` status, `supervisor/CLAUDE.md` |
