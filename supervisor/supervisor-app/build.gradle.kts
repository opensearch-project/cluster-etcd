// Entry point, configuration and wiring.
//
// It does hold logic worth testing after all, which was not the original expectation: the loop's failure
// policy (a pass that throws, a pass that overruns, a shutdown that will not complete) and the argument
// parsing both have decisions in them. MainTests runs the assembled program against the bundled filesystem
// backend, which is the only test anywhere that checks the thing actually starts.
plugins {
    application
}

val junitVersion: String by rootProject.extra

dependencies {
    implementation(project(":supervisor-core"))

    // Backend jars are deliberately absent: KeyValueStores discovers them through ServiceLoader, so nothing
    // here names etcd or ZooKeeper at compile time. To run against one, add it to the runtime classpath:
    //   ./gradlew :supervisor-app:installDist \
    //       -PstoreJars=org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT
    // Guarded, so it has no effect unless asked for.
    if (project.hasProperty("storeJars")) {
        project.property("storeJars").toString()
            .split(",")
            .map(String::trim)
            .filter(String::isNotEmpty)
            .forEach { add("runtimeOnly", it) }
    }

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("org.opensearch.supervisor.app.Main")
}
