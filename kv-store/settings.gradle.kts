rootProject.name = "opensearch-kv-store"

include("store-api")
include("store-zookeeper")
include("store-etcd")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Fallback so that --offline works: some artifacts are present in ~/.m2 but not in the Gradle
        // module cache. Gradle tries repositories in order, so this only matters when mavenCentral
        // cannot supply a module.
        mavenLocal()
    }
}
