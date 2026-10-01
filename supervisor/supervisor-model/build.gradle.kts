// Records, identifiers, key layout and JSON codecs. Nothing else.
//
// This module must never depend on store-api. That is not a style preference: DESIGN.md §1 claims the
// diff is a pure function of an observation, and a module that cannot see a KeyValueStore cannot quietly
// read a key it forgot to observe. The module graph is what makes that claim checkable at build time
// rather than at review time — so if something here seems to need the store, the design is wrong, not
// the dependency list.
plugins {
    `java-library`
}

val jacksonVersion: String by rootProject.extra
val junitVersion: String by rootProject.extra

dependencies {
    api("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
