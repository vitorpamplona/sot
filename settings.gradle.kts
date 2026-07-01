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

rootProject.name = "vespa-search"

include(":common-query")
include(":indexer")
// added as they are built: ":http-api", ":search-relay", ":cli"
