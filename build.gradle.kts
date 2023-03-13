import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import java.net.URI


plugins {
    kotlin("jvm") version "1.7.20"
    kotlin("plugin.serialization") version "1.7.20"
    `maven-publish`
}

group = "com.zamna"
version = "0.5"

val kotestVersion = "5.5.4"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.4.1")

    implementation("io.github.microutils:kotlin-logging-jvm:3.0.4")
    implementation("org.slf4j:slf4j-api:2.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.5")

    implementation("com.rabbitmq:amqp-client:5.16.0")

    // add rabbit mq
    testImplementation("io.kotest:kotest-runner-junit5:${kotestVersion}")
    testImplementation("io.kotest:kotest-assertions-core:${kotestVersion}")
    testImplementation("io.kotest:kotest-assertions-json:${kotestVersion}")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}

publishing {

    repositories {
        maven {
            name = "GitHubPackages"
            url = URI.create("https://maven.pkg.github.com/vchain-dev/kotask")
            credentials {
                username = System.getenv("GITHUB_ACTOR")
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }

    publications {
        create<MavenPublication>("kotask") {
            groupId = "com.zamna"
            artifactId = "kotask"
            version = "0.5"
            from(components["java"])
        }
    }
}