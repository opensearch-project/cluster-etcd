/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Assembles one Docker build context per image under build/images/<image>/: the Dockerfile and what it copies.
// No image is built here; bin/build-images.sh runs `docker build` on these directories.
//
// This is deployment wiring, so it names the etcd backend, which nothing in the components may do (D6.2).

// java-base for the JVM ecosystem's attribute rules (a library built for Java 11 suits a Java 21 consumer), not to
// compile anything.
plugins {
    `java-base`
}

// The same property, read the same way, as data-plane/build.gradle, so the plugin and the distribution it is
// installed into cannot be taken at different versions.
val opensearchVersion: String = System.getProperty("opensearch.version", "3.10.0-SNAPSHOT")
val etcdBackend = "org.opensearch.controlplane:store-etcd:0.1.0-SNAPSHOT"

fun Configuration.javaRuntime() {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
        attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        // Guava publishes a JRE and an Android variant and needs this to choose.
        attribute(TargetJvmEnvironment.TARGET_JVM_ENVIRONMENT_ATTRIBUTE, objects.named(TargetJvmEnvironment.STANDARD_JVM))
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 21)
    }
}

// One resolution per service, so Gradle settles a version conflict between the service and the backend (both
// bring Netty) as it does for `-PstoreJars=`, rather than two versions landing side by side in lib/.
val controlPlane by configurations.creating { javaRuntime() }
val supervisor by configurations.creating { javaRuntime() }
// The backend for the data node, which goes into the plugin's directory, minus what the node already has.
val nodeBackend by configurations.creating { javaRuntime() }
val opensearchServer by configurations.creating { javaRuntime() }

// Every Netty module in the control plane's image at one version. The control plane uses Netty 4.2 and jetcd's gRPC
// brings 4.1 for the modules the control plane does not name, and conflict resolution is per module, so lib/ held
// netty-codec 4.1 beside 4.2's netty-codec-base and netty-codec-compression: 192 classes twice. A wildcard classpath
// picks whichever copy the directory lists first, which differs between image builders; with the 4.1 copies every
// HTTP request was closed unanswered. stageControlPlane and stageSupervisor refuse a classpath with two Netty versions.
//
// At the control plane's own version (nettyVersion in control-plane/build.gradle.kts); keep the two equal. Fetching
// the 4.2 builds of jetcd's Netty modules needs the network once. -Pdeploy.nettyVersion overrides it.
val nettyVersion: String = providers.gradleProperty("deploy.nettyVersion").getOrElse("4.2.17.Final")

dependencies {
    controlPlane(platform("io.netty:netty-bom:$nettyVersion"))
    controlPlane("org.opensearch.controlplane:server:0.1.0-SNAPSHOT")
    controlPlane(etcdBackend)
    supervisor("org.opensearch.supervisor:supervisor-app:0.1.0-SNAPSHOT")
    supervisor(etcdBackend)
    nodeBackend(etcdBackend)
    opensearchServer("org.opensearch:opensearch:$opensearchVersion")
}

fun moduleKey(artifact: org.gradle.api.artifacts.result.ResolvedArtifactResult): String =
    when (val id = artifact.id.componentIdentifier) {
        is org.gradle.api.artifacts.component.ModuleComponentIdentifier -> "${id.group}:${id.module}"
        is org.gradle.api.artifacts.component.ProjectComponentIdentifier -> "project:${id.projectName}"
        else -> id.displayName
    }

// The jar-hell rule from kv-store/BACKENDS.md: a jar in a plugin's directory must not duplicate a class the node
// already loads. Everything the server depends on is in the distribution's lib/, so it is dropped by module here,
// as OpenSearch's own plugin build drops a plugin's compileOnly dependencies from its zip. store-api is dropped
// because the plugin zip bundles it. The Dockerfile checks the result against the real lib/ as well.
val nodeBackendJars = nodeBackend.incoming.artifacts.resolvedArtifacts.zip(
    opensearchServer.incoming.artifacts.resolvedArtifacts
) { backend, server ->
    // javax.annotation-api, which gRPC brings, holds the same classes as the jakarta.annotation-api 1.3.x the node
    // has: one library under two coordinates, which matching by module cannot see.
    val provided = server.map(::moduleKey).toSet() + "project:store-api" + "javax.annotation:javax.annotation-api"
    backend.filter { moduleKey(it) !in provided }.map { it.file }
}

val images = layout.buildDirectory.dir("images")

// A wildcard classpath with two versions of one library is order-dependent, so refuse it for Netty, where it bit.
fun Sync.requireOneNettyVersion(configuration: Configuration) {
    val versions = configuration.incoming.artifacts.resolvedArtifacts.map { artifacts ->
        artifacts.mapNotNull { it.id.componentIdentifier as? org.gradle.api.artifacts.component.ModuleComponentIdentifier }
            .filter { it.group == "io.netty" }
            .map { "${it.module}:${it.version}" }
    }
    doFirst {
        val found = versions.get()
        if (found.map { it.substringAfter(':') }.toSet().size > 1) {
            throw GradleException("${configuration.name} has more than one Netty version: ${found.sorted()}")
        }
    }
}

val stageControlPlane by tasks.registering(Sync::class) {
    group = "deploy"
    description = "Stages the control plane image: its runtime classpath and the etcd backend"
    requireOneNettyVersion(controlPlane)
    into(images.map { it.dir("control-plane") })
    from("images/control-plane")
    into("lib") { from(controlPlane) }
}

val stageSupervisor by tasks.registering(Sync::class) {
    group = "deploy"
    description = "Stages the supervisor image: its runtime classpath and the etcd backend"
    requireOneNettyVersion(supervisor)
    into(images.map { it.dir("supervisor") })
    from("images/supervisor")
    into("lib") { from(supervisor) }
}

// ------------------------------------------------------------------------------------------------------------
// The data node. The distribution and two of its plugins come from an OpenSearch checkout, built there at the
// version above, because no published OpenSearch is that version. Not built from here: it is another
// repository, and its build takes minutes.

val opensearchCheckout = providers.gradleProperty("opensearch.checkout")
val archSuffix = when (System.getProperty("os.arch")) {
    "aarch64", "arm64" -> "arm64"
    else -> "x64"
}
val imageArch: String = providers.gradleProperty("image.arch").getOrElse(archSuffix)
val distributionProject = if (imageArch == "arm64") "linux-arm64-tar" else "linux-tar"
val pluginBuildVersion = opensearchVersion.substringBefore('-') + ".0" +
    (if (opensearchVersion.contains('-')) "-" + opensearchVersion.substringAfter('-') else "")

fun fromCheckout(relative: String): Provider<File> = opensearchCheckout.map { checkout ->
    val file = File(checkout, relative)
    if (!file.isFile) {
        throw GradleException(
            "$file does not exist. Build it in the OpenSearch checkout at $opensearchVersion:\n" +
                "  ./gradlew :distribution:archives:$distributionProject:assemble " +
                ":plugins:repository-s3:assemble :plugins:ingestion-kafka:assemble"
        )
    }
    file
}

val stageDataNode by tasks.registering(Sync::class) {
    group = "deploy"
    description = "Stages the data node image: an OpenSearch distribution, the plugins and the etcd backend"
    dependsOn(gradle.includedBuild("data-plane").task(":plugin:bundlePlugin"))
    dependsOn(gradle.includedBuild("data-plane").task(":plugin:ingestionFsBundle"))
    doFirst {
        if (!opensearchCheckout.isPresent) {
            throw GradleException("Set -Popensearch.checkout=/path/to/OpenSearch, built at $opensearchVersion")
        }
    }

    into(images.map { it.dir("data-node") })
    from("images/data-node")
    from(fromCheckout("distribution/archives/$distributionProject/build/distributions/" +
        "opensearch-min-$opensearchVersion-linux-$imageArch.tar.gz")) {
        rename { "opensearch.tar.gz" }
    }
    // Installed in this order by the Dockerfile; the names are what it expects.
    into("plugins") {
        from("../data-plane/plugin/build/distributions/opensearch-data-plane-$pluginBuildVersion.zip") {
            rename { "opensearch-data-plane.zip" }
        }
        from("../data-plane/plugin/build/ingestion-fs/ingestion-fs-$opensearchVersion.zip") {
            rename { "ingestion-fs.zip" }
        }
        from(fromCheckout("plugins/repository-s3/build/distributions/repository-s3-$opensearchVersion.zip")) {
            rename { "repository-s3.zip" }
        }
        from(fromCheckout("plugins/ingestion-kafka/build/distributions/ingestion-kafka-$opensearchVersion.zip")) {
            rename { "ingestion-kafka.zip" }
        }
    }
    into("backend") { from(nodeBackendJars) }
}

tasks.register("stage") {
    group = "deploy"
    description = "Stages every image's Docker build context under build/images"
    dependsOn(stageControlPlane, stageSupervisor, stageDataNode)
}
