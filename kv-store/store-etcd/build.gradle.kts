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

dependencies {
    implementation(project(":store-api"))
    implementation("io.etcd:jetcd-core:$jetcdVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // The shared store contract, so this backend is held to the same behaviour as every other.
    testImplementation(testFixtures(project(":store-api")))
}

tasks.test {
    // The contract suite needs a real cluster. Point it at one to run those tests:
    //   ./gradlew :store-etcd:test -Dcontrolplane.etcd=localhost:2379
    systemProperty("controlplane.etcd", providers.systemProperty("controlplane.etcd").getOrElse(""))
}
