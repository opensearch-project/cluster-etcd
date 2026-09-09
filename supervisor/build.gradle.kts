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
    }
}

extra["jacksonVersion"] = jacksonVersion
extra["junitVersion"] = junitVersion
