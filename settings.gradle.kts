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
