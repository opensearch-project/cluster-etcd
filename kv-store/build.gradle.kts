// Imported rather than fully qualified: inside a build script `java` resolves to the java extension,
// not the java package, so `java.util.concurrent.TimeUnit` fails to compile with
// "Unresolved reference 'util'".
import java.util.concurrent.TimeUnit

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

        // Tell Testcontainers where Docker actually is.
        //
        // It finds the daemon on its own only when the socket is at the conventional path. Colima, Rancher
        // Desktop, and a Docker Desktop with the default socket disabled all put it
        // somewhere else, and the symptom is the integration tests skipping on a machine where `docker` works
        // perfectly -- which is exactly what happened here.
        //
        // The Docker CLI already knows the answer, so this asks it rather than asking the developer. Set
        // DOCKER_HOST yourself and this defers to it.
        doFirst {
            if (System.getenv("DOCKER_HOST").isNullOrBlank()) {
                val host = dockerHostFromContext()
                if (host != null) {
                    environment("DOCKER_HOST", host)
                    if (host.startsWith("unix://")) {
                        // Ryuk, the sidecar that reaps containers afterwards, talks to the daemon through a
                        // socket bind-mounted into it, and needs telling which path to mount. Without this it
                        // starts and cannot connect, which fails every container rather than skipping.
                        environment("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
                    }
                    logger.info("Testcontainers will use DOCKER_HOST={}", host)
                }
            }
        }
    }
}

extra["junitVersion"] = junitVersion

/**
 * The current Docker context's endpoint, or null if it cannot be determined.
 *
 * Reads local configuration only -- no daemon connection -- so it answers even when the daemon is down, and
 * returns null rather than failing when the CLI is absent. A null here means the integration tests skip and
 * say why, which is the right outcome on a machine without Docker.
 */
fun dockerHostFromContext(): String? =
    try {
        val process = ProcessBuilder(
            "docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"
        ).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().use { it.readText() }.trim()
        val finished = process.waitFor(10, TimeUnit.SECONDS)
        if (finished && process.exitValue() == 0 && output.isNotBlank() && output.contains("://")) {
            output
        } else {
            null
        }
    } catch (unavailable: Exception) {
        null
    }
