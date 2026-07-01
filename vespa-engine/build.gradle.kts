plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
    // The write side (VespaClient/VespaProjection) maps Quartz events -> Vespa docs
    // and consumes the event store's change feed.
    implementation(libs.quartz)
    implementation(libs.kotlinx.coroutines)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
