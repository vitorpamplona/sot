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

include(":config")
include(":event-store")
include(":vespa")
include(":indexer")
include(":http")
include(":relay")
include(":cli")

// v2 — the negentropy-first rewrite (see v2/README.md). Shares this build's
// version catalog, wrapper, and formatting; replaces the modules above when done.
include(":v2:vespa")
include(":v2:store")
include(":v2:relay")
include(":v2:profile")
