rootProject.name = "opensearch-control-plane"

include("codegen")
include("server")

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
