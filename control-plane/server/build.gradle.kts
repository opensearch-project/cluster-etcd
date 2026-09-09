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

dependencies {
    codegenClasspath(project(":codegen"))

    // The persistence SPI. Backends are NOT dependencies: they are jars an operator adds to the
    // runtime classpath, found through ServiceLoader. Naming one here would defeat the point.
    implementation(project(":store-api"))

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
    testImplementation(testFixtures(project(":store-api")))
}

application {
    mainClass.set("org.opensearch.controlplane.Main")
}
