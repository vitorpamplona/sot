plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    application
}

dependencies {
    implementation(libs.quartz)
    implementation(libs.okhttp)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    // Quartz's SQLiteEventStore uses the bundled Android-x SQLite driver.
    implementation(libs.androidx.sqlite.bundled)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass.set("com.vitorpamplona.sot.indexer.MainKt")
}
