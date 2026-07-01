import com.diffplug.gradle.spotless.SpotlessExtensionPredeclare

plugins {
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
                // We put a file-overview KDoc above the first declaration; that rule forbids it.
                ktlint(ktlintVersion).editorConfigOverride(
                    mapOf("ktlint_standard_no-consecutive-comments" to "disabled"),
                )
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
