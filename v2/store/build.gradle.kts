plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // The store implements Quartz's IEventStore on top of :v2:vespa's engine
    // port — both appear in its public API.
    api(libs.quartz)
    api(project(":v2:vespa"))
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":v2:vespa")))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
