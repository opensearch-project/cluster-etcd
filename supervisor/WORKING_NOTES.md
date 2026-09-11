# Working notes for this environment

Not about the supervisor's design — see `REQUIREMENTS.md` for that. This is the set of things that cost
hours to discover while building the control plane next door, written down so they cost minutes here.

## The sandbox blocks more than it looks like it does

- **Gradle cannot run.** It fails creating its file-lock service, because binding a socket is denied.
  Compile and test with `javac` and a hand-rolled JUnit launcher instead; every dependency is already in
  `~/.gradle/caches/modules-2/files-2.1` and `~/.m2/repository`. Ask a human to run `./gradlew build`, and
  report honestly that the Gradle build is unverified.

  **Now solved for this repository: `tools/jbuild.sh`.** It resolves jars from both local caches, compiles
  every module in dependency order with `--release 21`, and runs the tests through `tools/JUnitRunner.java`.
  `./gradlew --version` does work, which is misleading — the failure only appears once a task needs the
  lock service:

  ```
  > Could not create service of type FileLockContentionHandler
     > java.net.SocketException: Operation not permitted
  ```

  Two things the runner does on purpose: it **fails when it discovers zero tests**, because silently
  passing nothing looks exactly like a green build and there is no working Gradle here to contradict it;
  and it compiles `store-api` from `../opensearch-kv-store/store-api/src` rather than using that build's
  jars, so a change over there cannot go stale here.

- **The Java 21 toolchain is `temurin-21.0.12.1`.** With several JDKs installed and a newer default
  `java` on the path, both builds have to be
  explicit about which one they want. Gradle's auto-detection finds an installed 21 without configuration;
  `jbuild.sh` asks `/usr/libexec/java_home -v 21`, or honours `JAVA21_HOME`. Prefer a real 21 toolchain to
  `--release 21` on a newer `javac` — `--release` gets the language level and the API surface right but
  still runs a newer compiler and a newer JVM, which is approximately the build rather than the build.

- **No listening sockets.** `bind()` is `EPERM`, so the service cannot be started locally and no embedded
  test server (ZooKeeper's `TestingServer`, an etcd test container) can run either.
- **No raw TCP, even to localhost.** `connect()` to `127.0.0.1` on any port is `EPERM`. Outbound traffic is
  forced through a local HTTP forwarding proxy, which is why `curl` to `localhost:9200` works and a socket
  to the same port does not.
- **No Unix-domain sockets either.** `connect()` to an `AF_UNIX` path is also `EPERM`, so the Docker socket
  is out of reach even when you know exactly where it is. **No amount of cleverness runs a container from
  in here.** Container-based tests are for CI and for a human, and must skip cleanly without one.
- **That proxy silently truncates response bodies** and rewrites `Content-Length` to match, so a truncated
  body parses like a complete one. The cut-off varies wildly — 115 bytes in one request, 11,637 in
  another — so verify rather than trusting a body that happens to be valid JSON. `?filter_path=` to shrink
  a response is the usual workaround; comparing a live body as a *prefix* of yours is the other.
- **Maven Central is unreachable** (`HTTP 000`). A dependency not already in the local caches cannot be
  fetched, so a module needing one cannot even be compiled here.
- **Always use `curl --max-time 10`.** Without it a request can hang for the full tool timeout.
  `timeout(1)` is not installed.

### What this means for testing the supervisor

The same split that worked for the two store backends applies here. Put the risk in code that needs no
network — encoding, diffing, planning — and test that exhaustively in-process. Anything that needs a real
etcd, ZooKeeper or OpenSearch goes behind a condition that **skips** rather than fails, with the exact
command to run it in the class javadoc. See *Integration tests* below for the shape that ended up working.

The reconciliation loop is unusually friendly to this: a diff is a pure function of two observations, so
the interesting half can be tested with hand-built inputs and no store at all. Design for that from the
start — take observations as data, not as a store handle.

**One exception worth knowing:** etcd serves an HTTP/JSON gateway on its client port
(`POST /v3/kv/put|range|txn|deleterange`, base64 keys and values), and the proxy *does* carry it. That
makes etcd's real semantics probeable by hand even though a Java client cannot connect. ZooKeeper has no
equivalent, and neither does a raw data node. Its JSON also **omits false and zero fields**, so a failed
transaction has no `succeeded` key and a no-op delete has no `deleted` key.

**Local CLIs still work**, which is worth remembering: they read local files rather than opening sockets.
`docker context inspect` answers correctly in here even though the socket it names cannot be opened, and
that is how `DOCKER_HOST` gets resolved — see below.

### Integration tests: copy the pattern from the store backends

`../opensearch-kv-store` has this solved, and the supervisor will want the same shape the moment it tests
against a real etcd, ZooKeeper or OpenSearch. Three sources in order:

1. `-Dcontrolplane.<thing>=host:port` if set — a server already running.
2. A **Testcontainers** container if Docker is available. This is what CI uses; no configuration.
3. Otherwise **skip**, and *say why on stderr*.

Two hard-won details in that implementation, both worth stealing rather than rediscovering:

- **The `@EnabledIf` condition method must never throw.** A throwing condition does not disable the tests,
  it *fails the whole class*. Catch `Throwable` — normally wrong, right here — and answer false.
- **Then print the reason.** Swallowing it produced a worse bug than it fixed: Docker was running, the
  tests skipped anyway, and there was nothing to go on. `@EnabledIf` cannot carry a computed message into
  its `disabledReason`, so the explanation has to go to stderr.

And the reason Docker was not found: **Testcontainers only locates the daemon when the socket is at the
conventional path.** Colima, Rancher Desktop, other Docker-compatible runtimes, and a Docker Desktop with the
default socket disabled all put it elsewhere. The kv-store build asks the CLI
(`docker context inspect --format '{{.Endpoints.docker.Host}}'`) and passes the answer to the test JVM as
`DOCKER_HOST`, plus `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` so Ryuk can reach the daemon from inside its
container. **In the build, not a dotfile** — CI and the next clone do not read anyone's shell profile.

**And `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE` must stay `/var/run/docker.sock`, even when `DOCKER_HOST` says
the socket is somewhere else.** Learned by "fixing" it: the discovered path looked obviously more correct, and
it broke Ryuk with `Container startup failed for image testcontainers/ryuk` on exactly the runtimes the
discovery exists for. The two values answer different questions. `DOCKER_HOST` is where *this process* reaches
the daemon, on the host. The override is a bind-mount source, resolved by the **container runtime inside
whatever VM it runs containers in** — and a runtime whose client socket lives at a host-specific path still
presents the daemon at the conventional path in there, while the host-specific path does not exist in that
namespace at all. Two paths to one socket, and only one of them means anything to a container.

## Verify against something real, and diff afterwards

The habit that caught the most bugs next door: after implementing anything, run the same sequence of
requests through your own code and against a real server, print one line each, and compare. It found real
defects in code that had already passed its own unit tests, because unit tests and implementation share
the author's assumptions — if you misread what a field means, both are wrong together and agree.

For the supervisor this translates to: **do not trust a passing test that you wrote against your own
model of etcd, ZooKeeper or OpenSearch.** Probe the real thing.

## Gradle Kotlin DSL

One trap that cost a build failure and gives a misleading error: **inside a build script, `java` resolves
to the `java` extension the Java plugin adds, not the `java` package.** So
`java.util.concurrent.TimeUnit.SECONDS` fails with *Unresolved reference 'util'* — the message points at the
second segment when the problem is the first. Import the type at the top of the file instead. Type
references like `JavaPluginExtension` and `JavaCompile` are unaffected.

## Git

Commits are signed, so `git commit` blocks waiting for the key to be authorised and looks exactly like a
hung command. Write the message to a file and use `git commit -F <file>`, so a retry does not depend on
re-piping. If it times out, **check `git log` before doing anything else** — the commit frequently landed
anyway. Verify with `git log --format='%h %G? %s'`; `G` means a good signature.

Follow the OpenSearch DCO convention: a `Signed-off-by:` trailer.

Never `git add -A` blindly. Scratch files and generated, deployment-specific output get left in the tree
on purpose.

## Where the control plane's own reasoning lives

Read these before assuming anything about the other side of the store:

- `../opensearch-control-plane/README.md` — architecture, the three-plane model, persistence, and a
  "Considered divergences" table explaining every place it deliberately differs from OpenSearch. Also a
  "Still unverified" list, which is the honest inventory of what is not known.
- `../opensearch-control-plane/docs/where-the-hard-parts-are.md` — the remaining operations sorted by what
  makes them hard, with several "this looked easy and is not" write-ups.
- `git log` in that repo — commit messages carry the reasoning at length, deliberately.
