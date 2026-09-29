plugins {
    java
    application
}

val nettyVersion: String by rootProject.extra
val jacksonVersion: String by rootProject.extra
val junitVersion: String by rootProject.extra

// ---------------------------------------------------------------------------
// Route table generation
//
// :codegen reads the pinned spec plus the scope file and writes GeneratedRoutes
// into build/generated. The generated sources are a build output, not committed,
// so the spec and the scope file remain the single source of truth for the API
// surface. Regenerating is how you pick up a spec bump.
// ---------------------------------------------------------------------------
val generatedSourcesDir = layout.buildDirectory.dir("generated/sources/routes/java")
val scopeReportFile = layout.buildDirectory.file("reports/control-plane-scope.md")

// A dedicated configuration rather than reaching into :codegen's source sets,
// which Gradle 9 discourages and isolated projects forbids.
val codegenClasspath: Configuration by configurations.creating

val generateRoutes by tasks.registering(JavaExec::class) {
    group = "build"
    description = "Generates the control plane route table from the OpenSearch API specification."

    val specFile = rootProject.layout.projectDirectory.file("spec/opensearch-openapi.yaml")
    val scopeFile = rootProject.layout.projectDirectory.file("spec/control-plane-scope.yaml")

    inputs.file(specFile)
    inputs.file(scopeFile)
    outputs.dir(generatedSourcesDir)
    outputs.file(scopeReportFile)

    classpath = codegenClasspath
    mainClass.set("org.opensearch.controlplane.codegen.SpecCodegen")

    argumentProviders.add(CommandLineArgumentProvider {
        listOf(
            "--spec", specFile.asFile.absolutePath,
            "--scope", scopeFile.asFile.absolutePath,
            "--out", generatedSourcesDir.get().asFile.absolutePath,
            "--report", scopeReportFile.get().asFile.absolutePath
        )
    })
}

sourceSets["main"].java.srcDir(generateRoutes.map { generatedSourcesDir.get() })

// ---------------------------------------------------------------------------
// Optional store backends on the runtime classpath
//
// Backends are not dependencies of this module: :server names none of them, which is the whole point of
// the provider seam. But that left no way to actually run against one, so `--store zk://...` failed with
// "no store provider for scheme [zk]" and `--help` listed only the filesystem store.
//
// Any coordinate given here is added to the runtime classpath only. Deliberately a coordinate rather than
// a shorthand like "zk": a shorthand would mean this file naming the backends it knows about, which is
// the coupling the seam exists to avoid. Set it once in gradle.properties and forget it:
//
//     storeJars=org.opensearch.controlplane:store-zookeeper:0.1.0-SNAPSHOT
//
// or per invocation:
//
//     ../gradlew :server:run -PstoreJars=org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT
// ---------------------------------------------------------------------------
val storeJars: List<String> = ((findProperty("storeJars") as String?) ?: "")
    .split(",")
    .map(String::trim)
    .filter { it.isNotEmpty() }

dependencies {
    codegenClasspath(project(":codegen"))

    storeJars.forEach { coordinate -> runtimeOnly(coordinate) }

    // The persistence SPI, from the shared ../kv-store build. A coordinate rather than a
    // project(...) reference because it is a separate build; Gradle substitutes the local project via the
    // includeBuild in settings.gradle.kts, so there is no publish step.
    //
    // Backends are NOT dependencies: they are jars an operator adds to the runtime classpath, found
    // through ServiceLoader. Naming one here would defeat the point.
    implementation("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT")

    implementation("io.netty:netty-codec-http:$nettyVersion")
    implementation("io.netty:netty-handler:$nettyVersion")
    implementation("io.netty:netty-transport:$nettyVersion")
    implementation("io.netty:netty-buffer:$nettyVersion")
    implementation("io.netty:netty-common:$nettyVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    // The capabilities file is YAML, matching the scope file.
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:$jacksonVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // InMemoryKeyValueStore, which most handler tests run against.
    testImplementation(testFixtures("org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT"))
}

application {
    mainClass.set("org.opensearch.controlplane.Main")
}
