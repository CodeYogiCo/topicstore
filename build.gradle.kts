plugins {
    kotlin("jvm") version "1.9.25" apply false
    kotlin("multiplatform") version "1.9.25" apply false
    kotlin("kapt") version "1.9.25" apply false
    kotlin("plugin.serialization") version "1.9.25" apply false
    id("io.micronaut.application") version "4.4.4" apply false
    // Micronaut framework version is set in gradle.properties: micronautVersion
}

allprojects {
    group = "co.codeyogi.topicstore"
    version = "0.1.0"
}
