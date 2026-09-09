// Observe, diff, plan, apply, leases, the balancer and the loop. The only module that talks to the store.
plugins {
    `java-library`
}

val junitVersion: String by rootProject.extra

dependencies {
    api(project(":supervisor-model"))

    // Group, name and version must match exactly, or the composite-build substitution declared in
    // settings.gradle.kts misses and this resolves against a repository instead of ../opensearch-kv-store.
    api("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // InMemoryKeyValueStore and KeyValueStoreContractTests, so this service's tests run against the same
    // in-memory store the control plane's do rather than a second guess at the same semantics.
    testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
}
