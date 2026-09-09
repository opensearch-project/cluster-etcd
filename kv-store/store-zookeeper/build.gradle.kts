// The ZooKeeper backend. A standalone jar: it depends on the persistence SPI and on Curator, and
// nothing depends on it -- an operator adds it to the runtime classpath and selects it with
// --store zk://host:2181/root.
plugins {
    `java-library`
}

val junitVersion: String by rootProject.extra
val curatorVersion = "5.2.0"
val zookeeperVersion = "3.9.5"

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
}

tasks.test {
    // The contract suite needs a real ensemble. Point it at one to run those tests:
    //   ./gradlew :store-zookeeper:test -Dcontrolplane.zookeeper=localhost:2181
    systemProperty(
        "controlplane.zookeeper",
        providers.systemProperty("controlplane.zookeeper").getOrElse("")
    )
}
