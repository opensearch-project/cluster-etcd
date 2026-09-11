// Integration tests against a real store. No production code — this module exists so that jetcd, gRPC,
// protobuf and a Docker client stay off every other module's classpath.
//
// Everything else in this repository tests against InMemoryKeyValueStore with a controllable clock, which is
// the right default: those tests are fast, deterministic, and can drive a whole fleet through fifty passes in
// milliseconds. What they cannot do is tell you whether the assumptions the design rests on are true of a real
// backend. DESIGN.md §12 reasons carefully about torn reads from a non-atomic scanPrefix without ever having
// observed one; liveness is derived from revisions changing, which nothing has verified against a store that
// assigns them for real; and every concurrency test so far has been single-threaded with a fake clock, which
// is the one shape a lease is not interesting in.
//
// So these tests are deliberately *not* a second copy of the unit suite. They check the things only a real
// backend and a real clock can answer.
plugins {
    `java-library`
}

val junitVersion: String by rootProject.extra
val testcontainersVersion = "1.21.4"

dependencies {
    testImplementation(project(":supervisor-core"))
    testImplementation(project(":fake-node"))

    // The one place in this repository that names a backend. Everywhere else resolves one through
    // ServiceLoader, which is the point of the SPI; here the tests have to guarantee specific ones are present.
    //
    // Both, deliberately. The suites are backend-agnostic and each runs against every backend on the classpath,
    // which is the only way to answer "are we relying on a guarantee only one of these makes?" -- and the two
    // differ on exactly the guarantee the design worries about most: etcd answers a prefix scan atomically and
    // ZooKeeper walks the subtree with a read per key.
    testImplementation("org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT")
    testImplementation("org.opensearch.controlplane:store-zookeeper:0.1.0-SNAPSHOT")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // Starts a real etcd in Docker when one is not already running, so CI needs Docker and no configuration.
    // Core only: etcd has no official Testcontainers module and GenericContainer is all a single node needs.
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
}

tasks.test {
    // With Docker available these start their own etcd and need no configuration. Point them at a cluster you
    // are already running instead -- much faster, and sometimes the point:
    //   ./gradlew :supervisor-integ:test -Dsupervisor.etcd=localhost:2379 -Dsupervisor.zookeeper=localhost:2181
    // Each backend is independent: point one at a running server and the other still uses a container. The
    // images are overridable for a different version or an unreachable registry:
    //   -Dsupervisor.etcd.image=... -Dsupervisor.zookeeper.image=...
    systemProperty("supervisor.etcd", providers.systemProperty("supervisor.etcd").getOrElse(""))
    systemProperty("supervisor.etcd.image", providers.systemProperty("supervisor.etcd.image").getOrElse(""))
    systemProperty("supervisor.zookeeper", providers.systemProperty("supervisor.zookeeper").getOrElse(""))
    systemProperty("supervisor.zookeeper.image",
        providers.systemProperty("supervisor.zookeeper.image").getOrElse(""))

    // These are minutes-long when they run, because leases and liveness are measured in wall-clock seconds and
    // there is no clock to advance. Not part of a quick edit-test loop.
    testLogging.showStandardStreams = true
}
