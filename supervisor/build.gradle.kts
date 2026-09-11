// Imported rather than fully qualified: inside a build script `java` resolves to the java extension, not the
// java package, so `java.util.concurrent.TimeUnit` fails to compile with "Unresolved reference 'util'".
import java.util.concurrent.TimeUnit

plugins {
    java
}

// Versions are pinned to what resolves offline from the local Gradle/Maven caches, matching the control
// plane so that the two services and the shared store agree. Bump freely once you have network access.
val jacksonVersion = "2.22.1"
val junitVersion = "5.11.4"

allprojects {
    apply(plugin = "java")

    group = "org.opensearch.supervisor"
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
        // It finds the daemon on its own only when the socket sits at the conventional path. Colima, Rancher
        // Desktop, Apple's `container` tool and a Docker Desktop with the default socket disabled all put it
        // somewhere else, and the symptom is the integration tests *skipping* on a machine where `docker` works
        // perfectly. That is a bad failure mode: a green build that tested nothing.
        //
        // The Docker CLI already knows the answer, so ask it rather than asking the developer. An explicit
        // DOCKER_HOST wins, since somebody who set it meant it.
        doFirst {
            if (System.getenv("DOCKER_HOST").isNullOrBlank()) {
                val host = dockerHostFromContext()
                if (host != null) {
                    environment("DOCKER_HOST", host)
                    if (host.startsWith("unix://")) {
                        // Ryuk, the sidecar that reaps containers afterwards, reaches the daemon through a
                        // socket bind-mounted into it and has to be told which host path to mount. Without
                        // this it starts and cannot connect, which fails every container rather than skipping.
                        //
                        // Derived from the context rather than hardcoded to /var/run/docker.sock, which is the
                        // one path that is definitely wrong on the setups this whole block exists for: with
                        // Apple's `container` tool the socket is under ~/.config/container and /var/run/docker.sock
                        // does not exist at all, so naming it would point Ryuk at nothing.
                        environment("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", host.removePrefix("unix://"))
                    }
                    logger.lifecycle("Testcontainers will use DOCKER_HOST={}", host)
                }
            }
        }
    }
}

extra["jacksonVersion"] = jacksonVersion
extra["junitVersion"] = junitVersion

/**
 * The current Docker context's endpoint, or null if it cannot be determined.
 *
 * Reads local CLI configuration only -- no daemon connection -- so it answers even when the daemon is down, and
 * returns null rather than failing when the CLI is absent. Null means the integration tests skip and say why,
 * which is the right outcome on a machine without Docker.
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
