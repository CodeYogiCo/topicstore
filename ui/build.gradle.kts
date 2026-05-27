plugins {
    kotlin("multiplatform")
    kotlin("plugin.serialization")
}

version = "0.1.0"

repositories {
    mavenCentral()
}

kotlin {
    js(IR) {
        moduleName = "topicstore-ui"
        browser {
            commonWebpackConfig {
                outputFileName = "app.js"
            }
            testTask { enabled = false }
        }
        binaries.executable()
    }
    sourceSets {
        val jsMain by getting {
            dependencies {
                implementation(project.dependencies.platform("org.jetbrains.kotlin-wrappers:kotlin-wrappers-bom:1.0.0-pre.733"))
                implementation("org.jetbrains.kotlin-wrappers:kotlin-react")
                implementation("org.jetbrains.kotlin-wrappers:kotlin-react-dom")
                implementation("org.jetbrains.kotlin-wrappers:kotlin-emotion")
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
                implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")
            }
        }
    }
}
