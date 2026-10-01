// A data plane test double: registers, heartbeats, reports an inventory, and obeys its assignment.
//
// Not a testing nicety. It is the only way to exercise the loop before a data plane exists, and since it
// has to implement NODE_CONTRACT.md from the document alone, it is the first real review of whether that
// contract is implementable. Milestone 2 fills it in.
//
// It also publishes the contract's shared conformance suite as test fixtures. NODE_CONTRACT.md now has two
// independent implementations -- this one and the real plugin in ../data-plane -- and two
// implementations of one document format tested separately are two formats. The assertions belong in the
// repository that owns the contract, in the manner of KeyValueStoreContractTests in the store repository,
// so that a change to the document is a change to one suite both sides then have to pass.
//
// They live in this module rather than in supervisor-model because they need a KeyValueStore, and
// supervisor-model must never be able to see one.
plugins {
    application
    `java-test-fixtures`
}

val junitVersion: String by rootProject.extra

dependencies {
    implementation(project(":supervisor-model"))
    implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")

    // api rather than implementation throughout: a consumer of these fixtures writes a subclass whose
    // signatures mention KeyValueStore and NodeId, so both have to be on its compile classpath.
    testFixturesApi(project(":supervisor-model"))
    testFixturesApi("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
    testFixturesApi(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
    testFixturesApi(platform("org.junit:junit-bom:$junitVersion"))
    testFixturesApi("org.junit.jupiter:junit-jupiter-api")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
}

application {
    mainClass.set("org.opensearch.supervisor.fakenode.FakeNodeMain")
}
