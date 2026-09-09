// The persistence SPI: the KeyValueStore interface, the provider seam backends plug into, and the
// two implementations that need nothing but the JDK.
//
// Deliberately dependency-free. A backend jar depends on this and on its own client library; if this
// module grew a Jackson or Netty dependency, every backend would inherit it.
plugins {
    `java-library`
    // Publishes src/testFixtures as a separate artifact, which is how each backend module gets
    // KeyValueStoreContractTests and InMemoryKeyValueStore without depending on another module's tests.
    `java-test-fixtures`
}

val junitVersion: String by rootProject.extra

dependencies {
    testFixturesApi(platform("org.junit:junit-bom:$junitVersion"))
    testFixturesApi("org.junit.jupiter:junit-jupiter")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
