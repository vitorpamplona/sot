plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    implementation(project(":config")) // dbPath + relay-identity defaults
    implementation(libs.quartz) // EventStore / ObservableEventStore / DefaultIndexingStrategy
    // Quartz's SQLiteEventStore uses the bundled AndroidX SQLite driver at runtime.
    implementation(libs.androidx.sqlite.bundled)
}

kotlin {
    jvmToolchain(21)
}
