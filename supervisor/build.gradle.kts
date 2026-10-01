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
        // It finds the daemon on its own only when the socket sits at the conventional path. Several
        // Docker-compatible runtimes -- colima, Rancher Desktop, and a Docker Desktop with the default socket
        // disabled among them -- put it somewhere else, and the symptom is the integration tests *skipping* on a
        // machine where `docker` works perfectly. That is a bad failure mode: a green build that tested nothing.
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
                        // socket bind-mounted into it and has to be told which path to mount. Without this it
                        // starts and cannot connect, which fails every container rather than skipping.
                        //
                        // The conventional path, NOT the one from the context, even when the two differ -- and
                        // this looks wrong until you know why. The bind-mount source is resolved by the
                        // container runtime, inside whatever VM it runs containers in, not on the host running
                        // Gradle. A runtime whose client socket lives at some host-specific path still presents
                        // the daemon at the conventional path inside that VM, and the host-specific path does
                        // not exist there at all. Deriving this from DOCKER_HOST therefore breaks Ryuk on
                        // exactly the setups the surrounding block exists to support: tried, and it fails with
                        // "Container startup failed for image testcontainers/ryuk".
                        environment("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")
                    }
                    logger.lifecycle("Testcontainers will use DOCKER_HOST={}", host)
                }
            }
        }
    }
}


// ":check" and friends at this root cover every project, so the repository's root build can delegate to
// this build without listing its modules. Run from here, Gradle already selects the task in every project.
listOf("assemble", "check", "build", "clean").forEach { name ->
    tasks.named(name) { dependsOn(subprojects.map { "${it.path}:$name" }) }
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
