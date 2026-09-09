// A data plane test double: registers, heartbeats, reports an inventory, and obeys its assignment.
//
// Not a testing nicety. It is the only way to exercise the loop before a data plane exists, and since it
// has to implement NODE_CONTRACT.md from the document alone, it is the first real review of whether that
// contract is implementable. Milestone 2 fills it in.
plugins {
    application
}

val junitVersion: String by rootProject.extra

dependencies {
    implementation(project(":supervisor-model"))
    implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("org.opensearch.supervisor.fakenode.FakeNodeMain")
}
