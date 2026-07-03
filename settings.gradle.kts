pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}

rootProject.name = "sot"

// The product: Vespa IS the event store, NIP-50 is the API (see README.md).
include(":vespa")
include(":store")
include(":relay")
include(":profile")
include(":sync")
include(":cli")

// v1 — the frozen SQLite-store implementation, kept as the working reference
// until the root modules are validated against a real Vespa deployment and
// real relays. No new work lands here; delete the folder when v2 is proven.
include(":v1:config")
include(":v1:event-store")
include(":v1:vespa")
include(":v1:indexer")
include(":v1:http")
include(":v1:relay")
include(":v1:cli")
