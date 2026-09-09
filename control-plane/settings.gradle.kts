rootProject.name = "opensearch-control-plane"

include("codegen")
include("server")

// The persistence seam lives in its own build, because the supervisor (management plane) shares it and two
// implementations of the same revision semantics against the same keys is how two planes would silently
// disagree about state. Included as a composite build rather than consumed as a published artifact, so
// that editing both services together needs no publish step.
//
// This does mean the build depends on a path outside this repository; CI has to check out both. A
// submodule or a real published artifact removes that later without changing the module layout.
includeBuild("../opensearch-kv-store")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Fallback so that --offline works: some artifacts (the JUnit 1.11.4
        // platform set) are present in ~/.m2 but not in the Gradle module cache.
        // Gradle tries repositories in order, so this only comes into play when
        // mavenCentral cannot supply a module.
        mavenLocal()
    }
}
