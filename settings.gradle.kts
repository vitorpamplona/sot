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
include(":http-api")
// added as they are built: ":search-relay", ":cli"
