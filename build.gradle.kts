/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

// Lifecycle tasks that run the same task in every component. Each component's root task already covers its
// own subprojects, so nothing here lists modules. A single component or module is addressed by build name:
// `./gradlew :supervisor:supervisor-core:test`, `./gradlew :control-plane:server:run`.
listOf("assemble", "check", "build", "clean").forEach { name ->
    tasks.register(name) {
        group = if (name == "check") "verification" else "build"
        description = "Runs $name in every component."
        dependsOn(gradle.includedBuilds.map { it.task(":$name") })
    }
}
