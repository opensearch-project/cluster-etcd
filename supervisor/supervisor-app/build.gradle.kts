// Entry point, configuration and wiring. Holds no logic worth testing in isolation, on purpose.
plugins {
    application
}

val junitVersion: String by rootProject.extra

dependencies {
    implementation(project(":supervisor-core"))

    testImplementation(platform("org.junit:junit-bom:$junitVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("org.opensearch.supervisor.app.Main")
}
