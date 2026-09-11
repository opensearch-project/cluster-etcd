rootProject.name = "opensearch-supervisor"

include("supervisor-model")
include("supervisor-core")
include("supervisor-app")
include("fake-node")

// Integration tests, in their own module because of what they have to drag in. store-etcd brings jetcd,
// which brings gRPC and protobuf, and Testcontainers brings a Docker client; putting any of that on
// supervisor-core's test classpath would burden the module whose dependency hygiene this build actively
// asserts. Nothing else depends on this one, and it holds no production code.
include("supervisor-integ")

// The persistence seam is shared with the control plane and lives in its own build. Two implementations
// of the same revision semantics against the same keys is how the two planes would silently disagree
// about state, so there is one implementation and both services depend on it.
//
// Included as a composite build rather than consumed as a published artifact, so that editing the store
// and a service together needs no publish step. Substitution matches on group and name, so the
// coordinate in each module must stay exactly "org.opensearch.controlplane:store-api:0.1.0-SNAPSHOT" —
// change either and it resolves against a repository instead of the local project, and fails.
//
// The cost, recorded so nobody rediscovers it: this build depends on a path outside itself, so CI must
// check out both repositories. A submodule or a published artifact removes that later without changing
// the module layout.
includeBuild("../opensearch-kv-store")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Fallback so that --offline works: some artifacts (the JUnit 1.11.4 platform set) are present
        // in ~/.m2 but not in the Gradle module cache. Gradle tries repositories in order, so this only
        // comes into play when mavenCentral cannot supply a module.
        mavenLocal()
    }
}
