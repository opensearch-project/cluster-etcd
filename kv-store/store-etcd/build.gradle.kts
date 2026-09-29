// The etcd backend. A standalone jar: depends on the persistence SPI and on jetcd, and nothing depends
// on it -- an operator adds it to the runtime classpath and selects it with --store etcd://host:2379/prefix.
//
// jetcd brings gRPC and protobuf transitively, which is the heaviest of the backends and the reason this
// is a separate module rather than optional code in :server.
plugins {
    `java-library`
}

val junitVersion: String by rootProject.extra
val jetcdVersion = "0.8.5"
val testcontainersVersion = "1.21.4"

dependencies {
    implementation(project(":store-api"))
    implementation("io.etcd:jetcd-core:$jetcdVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // The shared store contract, so this backend is held to the same behaviour as every other.
    testImplementation(testFixtures(project(":store-api")))

    // Starts a real server in Docker when one is not already running, so CI needs Docker and no
    // configuration. Core only: neither backend has an official Testcontainers module, and GenericContainer
    // is all a single-node server needs.
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
}

tasks.test {
    // The contract suite needs a real cluster. With Docker available it starts one itself and needs no
    // configuration, which is how CI runs. Set this to point it at a cluster you are already running
    // instead -- faster, and sometimes the point:
    //   ../gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379
    //
    // The image is overridable too, for a different version or an unreachable registry:
    //   -Dcontrolplane.etcd.image=...
    systemProperty("controlplane.etcd", providers.systemProperty("controlplane.etcd").getOrElse(""))
    systemProperty("controlplane.etcd.image", providers.systemProperty("controlplane.etcd.image").getOrElse(""))
}
