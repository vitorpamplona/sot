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
