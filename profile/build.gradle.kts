plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // The trust projection decorates :vespa's EventIndex and parses NIP-85
    // events with Quartz — both appear in the wiring API.
    api(libs.quartz)
    api(project(":vespa"))
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":vespa")))
    // Tests drive the projection through the real store semantics.
    testImplementation(project(":store"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
