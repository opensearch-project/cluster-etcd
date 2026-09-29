plugins {
    java
}

// Versions are pinned to what resolves offline from the local Gradle/Maven
// caches. Bump freely once you have network access.
val nettyVersion = "4.2.17.Final"
val jacksonVersion = "2.22.1"
val junitVersion = "5.11.4"

allprojects {
    apply(plugin = "java")

    group = "org.opensearch.controlplane"
    version = "0.1.0-SNAPSHOT"

    extensions.configure<JavaPluginExtension> {
        toolchain {
            // Matches OpenSearch 3.x, so business logic ported from the server
            // compiles without language-level surprises.
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


// ":check" and friends at this root cover every project, so the repository's root build can delegate to
// this build without listing its modules. Run from here, Gradle already selects the task in every project.
listOf("assemble", "check", "build", "clean").forEach { name ->
    tasks.named(name) { dependsOn(subprojects.map { "${it.path}:$name" }) }
}

extra["nettyVersion"] = nettyVersion
extra["jacksonVersion"] = jacksonVersion
extra["junitVersion"] = junitVersion
