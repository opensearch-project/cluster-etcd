/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Stages the Docker build contexts for the Kubernetes deployment (see README.md). Its own build, and not
// included by the root, so that `./gradlew build` neither stages images nor needs an OpenSearch distribution:
//
//   ./gradlew -p deploy stage -Popensearch.checkout=/path/to/OpenSearch
//
// The components are included by relative path, as they include each other, so this resolves them from source.
rootProject.name = "clusterless-deploy"

includeBuild("../kv-store")
includeBuild("../control-plane")
includeBuild("../supervisor")
includeBuild("../data-plane")

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        mavenLocal()
    }
}
