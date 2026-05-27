plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.serialization")
    id("io.micronaut.application")
}

version = "0.1.0"

repositories {
    mavenCentral()
}

micronaut {
    runtime("netty")
    testRuntime("junit5")
    processing {
        incremental(true)
        annotations("co.codeyogi.topicstore.*")
    }
}

dependencies {
    kapt("io.micronaut:micronaut-http-validation")
    kapt("io.micronaut.serde:micronaut-serde-processor")
    kapt("io.micronaut.validation:micronaut-validation-processor")

    implementation("io.micronaut:micronaut-http-server-netty")
    implementation("io.micronaut:micronaut-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut.kotlin:micronaut-kotlin-extension-functions")
    implementation("io.micronaut.serde:micronaut-serde-jackson")
    implementation("io.micronaut.validation:micronaut-validation")
    implementation("jakarta.annotation:jakarta.annotation-api")

    implementation("org.jetbrains.kotlin:kotlin-stdlib")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.3")

    implementation("org.apache.kafka:kafka-clients:3.7.0")
    implementation("com.clickhouse:clickhouse-jdbc:0.6.3:all") {
        exclude(group = "org.slf4j")
    }

    runtimeOnly("ch.qos.logback:logback-classic:1.5.6")
    runtimeOnly("org.yaml:snakeyaml")

    testImplementation("io.micronaut.test:micronaut-test-junit5")
    testImplementation("io.micronaut:micronaut-http-client")
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
    testImplementation("org.testcontainers:kafka:1.19.8")
    testImplementation("org.testcontainers:clickhouse:1.19.8")
    testImplementation("org.awaitility:awaitility:4.2.1")
    testImplementation("org.assertj:assertj-core:3.25.3")
}

application {
    mainClass.set("co.codeyogi.topicstore.ApplicationKt")
}

java {
    sourceCompatibility = JavaVersion.toVersion("21")
    targetCompatibility = JavaVersion.toVersion("21")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "21"
        freeCompilerArgs += listOf("-Xjsr305=strict", "-opt-in=kotlin.RequiresOptIn")
    }
}

tasks.named<Test>("test") {
    useJUnitPlatform()
    systemProperty("micronaut.environments", "test")
}
