/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

rootProject.name = "cluster-etcd"

// Each component is a complete build of its own and still builds from its own directory (`../gradlew build`).
// Here they are included as a composite, so one invocation builds all of them, and a change to the store or
// the node contract is picked up by every build that consumes it without publishing anything.
//
// The components include each other by relative path (the data plane includes kv-store and supervisor, and
// so on); Gradle identifies an included build by its directory, so those resolve to these same builds.
includeBuild("kv-store")
includeBuild("control-plane")
includeBuild("supervisor")
includeBuild("data-plane")
