import com.diffplug.gradle.spotless.SpotlessExtensionPredeclare

plugins {
    // Load the Kotlin plugins once, on the root classpath (`apply false`): subprojects
    // applying them per-module would otherwise each get their own classloader copy
    // ("The Kotlin Gradle plugin was loaded multiple times in different subprojects").
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.diffplug.spotless)
}

val ktlintVersion = libs.versions.ktlint.get()

allprojects {
    apply(plugin = "com.diffplug.spotless")

    if (project === rootProject) {
        // Predeclare formatter dependencies once at the root (Spotless multi-project best practice).
        spotless { predeclareDeps() }
        configure<SpotlessExtensionPredeclare> {
            kotlin { ktlint(ktlintVersion) }
            kotlinGradle { ktlint(ktlintVersion) }
        }
    } else {
        spotless {
            kotlin {
                target("src/**/*.kt")
                ktlint(ktlintVersion)
                licenseHeaderFile(
                    rootProject.file(".spotless/copyright.kt"),
                    "@file:|package|import|class|object|sealed|open|interface|abstract ",
                )
            }
            kotlinGradle {
                target("*.gradle.kts")
                ktlint(ktlintVersion)
            }
        }
    }
}
