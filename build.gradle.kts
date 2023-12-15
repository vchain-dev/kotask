plugins {
    kotlin("jvm") version "1.9.0"
    kotlin("plugin.serialization") version "1.9.0"

    id("java-library")
    id("maven-publish")
    id("signing")
    id("org.jetbrains.dokka") version "1.8.10"
}

println("Java version for current gradle run: ${org.gradle.internal.jvm.Jvm.current()}")

group = "com.zamna"
version = "0.8.0"
description = "Kotlin asynchronous task framework using RabbitMQ."

val kotestVersion = "5.5.4"
val exposedVersion: String = "0.41.1"


val jvmTargetVersion = JavaLanguageVersion.of(18)

java {
    toolchain {
        languageVersion.set(jvmTargetVersion)
    }
}

kotlin {
    jvmToolchain {
        languageVersion.set(jvmTargetVersion)
    }
}

repositories {
    mavenCentral()
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}

tasks.jar {
    manifest {
        attributes(mapOf(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version
        ))
    }
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.6.4")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.5.0")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.4.0")

    implementation("io.github.oshai:kotlin-logging-jvm:5.1.0")
    implementation("org.slf4j:slf4j-api:2.0.5")
    implementation("ch.qos.logback:logback-classic:1.4.12")
    implementation("ch.qos.logback.contrib:logback-json-core:0.1.5")
    implementation("com.rabbitmq:amqp-client:5.18.0") {
        isTransitive = true
    }
    implementation("com.cronutils:cron-utils:9.2.0") {
        isTransitive = true
    }


    implementation("io.micrometer:micrometer-core:1.11.5")


    // postgresql (should be moved out of main bundle)
    implementation("org.jetbrains.exposed:exposed-core:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-dao:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-jdbc:$exposedVersion")
    implementation("org.jetbrains.exposed:exposed-java-time:$exposedVersion")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.postgresql:postgresql:42.6.0")

    implementation("com.azure:azure-messaging-servicebus:7.14.2")

    // add rabbit mq
    testImplementation("io.kotest:kotest-runner-junit5:5.5.5")
    testImplementation("io.kotest:kotest-assertions-core:5.5.5")
    testImplementation("io.kotest:kotest-assertions-json:5.5.5")
    testImplementation("io.kotest.extensions:kotest-extensions-testcontainers:1.3.4")
    testImplementation("org.testcontainers:postgresql:1.18.3")
    testImplementation("io.mockk:mockk:1.13.5")
}

tasks.publishToMavenLocal {
    dependsOn(tasks.jar)
}

tasks.publish {
    dependsOn(tasks.jar)
}

val sourcesJar = tasks.register<Jar>("sourcesJar") {
    dependsOn(tasks.classes)

    from(sourceSets["main"].allSource)
    archiveClassifier.set("sources")
}

val dokkaJavadocJar = tasks.register<Jar>("dokkaJavadocJar") {
    dependsOn(tasks.dokkaJavadoc)
    from(tasks.dokkaJavadoc.flatMap { it.outputDirectory })
    archiveClassifier.set("javadoc")
}

tasks.jar {
    dependsOn(dokkaJavadocJar)
    dependsOn(sourcesJar)
}

publishing {

    repositories {
        maven {
            name = "SonatypeMavenCentral"
            url = uri("https://s01.oss.sonatype.org/content/repositories/releases")
            credentials {
                username = System.getenv("MAVEN_CENTRAL_USERNAME")
                password = System.getenv("MAVEN_CENTRAL_PASSWORD")
            }
        }
    }

    publications {
        create<MavenPublication>("kotask") {
            groupId = "com.zamna"
            artifactId = project.name as String
            version = project.version as String

            from(components["kotlin"])

            artifact(dokkaJavadocJar)
            artifact(sourcesJar)

            pom {
                packaging = "jar"

                name.set(project.name as String)
                description.set(project.description as String)

                url.set("https://github.com/vchain-dev/kotask")

                scm {
                    url.set("https://github.com/vchain-dev/kotask")
                }

                issueManagement {
                    url.set("https://github.com/vchain-dev/kotask/issues")
                }

                licenses {
                    license {
                        name.set("MIT License")
                        url.set("https://github.com/vchain-dev/kotask/blob/main/LICENSE")
                    }
                }

                developers {
                    developer {
                        id.set("ilyatikhonov")
                        name.set("Ilya Tikhonov")
                    }
                    developer {
                        id.set("baitcode")
                        name.set("Ilia Batii")
                    }
                }
            }

        }
    }
}

//signing {
//    val signingKey: String? by project
//    val signingPassword: String? by project
//    useInMemoryPgpKeys(signingKey, signingPassword)
//
//    sign(publishing.publications["kotask"])
//}
