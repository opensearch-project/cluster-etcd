// The control plane's persistence seam, extracted so that more than one service can share it.
//
// This exists as its own build rather than as modules inside the control plane because the supervisor
// (management plane) needs the same KeyValueStore, and two implementations of the same revision semantics
// against the same keys is how two planes would silently disagree about state.
//
// Consumers include it as a composite build:
//
//     // settings.gradle.kts
//     includeBuild("../opensearch-kv-store")
//
//     // build.gradle.kts
//     implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")
//     testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
//
// Gradle substitutes the local projects for those coordinates, so there is no publish step while both
// services are being edited together. Publishing properly is a later step and does not require undoing
// this one.
plugins {
    java
}

// Kept identical to the control plane's, because composite-build substitution matches on group and name.
val junitVersion = "5.11.4"

allprojects {
    apply(plugin = "java")

    group = "org.opensearch.controlplane"
    version = "0.1.0-SNAPSHOT"

    extensions.configure<JavaPluginExtension> {
        toolchain {
            // Matches the control plane and OpenSearch 3.x.
            languageVersion.set(JavaLanguageVersion.of(21))
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Xlint:-serial", "-Xlint:-this-escape"))
        options.encoding = "UTF-8"
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}

extra["junitVersion"] = junitVersion
