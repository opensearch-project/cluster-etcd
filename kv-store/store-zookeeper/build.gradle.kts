// The ZooKeeper backend. A standalone jar: it depends on the persistence SPI and on Curator, and
// nothing depends on it -- an operator adds it to the runtime classpath and selects it with
// --store zk://host:2181/root.
plugins {
    `java-library`
}

val junitVersion: String by rootProject.extra
val curatorVersion = "5.2.0"
val zookeeperVersion = "3.9.5"
val testcontainersVersion = "1.21.4"

dependencies {
    implementation(project(":store-api"))

    // curator-framework brings curator-client and zookeeper transitively; zookeeper is named explicitly
    // because ZooKeeperKeys is validated against its PathUtils and that is a compile dependency, not an
    // incidental one.
    implementation("org.apache.curator:curator-framework:$curatorVersion")
    implementation("org.apache.zookeeper:zookeeper:$zookeeperVersion")

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
    // The contract suite needs a real ensemble. With Docker available it starts one itself and needs no
    // configuration, which is how CI runs. Set this to point it at a ensemble you are already running
    // instead -- faster, and sometimes the point:
    //   ./gradlew :store-zookeeper:test -Dcontrolplane.zookeeper=localhost:2181
    //
    // The image is overridable too, for a different version or an unreachable registry:
    //   -Dcontrolplane.zookeeper.image=...
    systemProperty("controlplane.zookeeper", providers.systemProperty("controlplane.zookeeper").getOrElse(""))
    systemProperty("controlplane.zookeeper.image", providers.systemProperty("controlplane.zookeeper.image").getOrElse(""))
}
