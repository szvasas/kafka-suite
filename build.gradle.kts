plugins {
    id("org.jetbrains.kotlin.jvm") version "1.4.21"
    `java-library`
    `maven-publish`
}

group = "dev.vasas"
version = "1.0.SNAPSHOT"

java {
    withJavadocJar()
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "kafka-suite"
            from(components["java"])
            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }
            pom {
                name.set("KafkaSuite")
                licenses {
                    license {
                        name.set("MIT License")
                    }
                }
                developers {
                    developer {
                        id.set("szvasas")
                        name.set("Szabolcs Vasas")
                        email.set("vasas@apache.org")
                    }
                }
                scm {
                    connection.set("git@github.com:szvasas/kafka-suite.git")
                }
            }
        }
    }
    repositories {
        maven {
            val releasesRepoUrl = uri("$buildDir/repos/releases")
            val snapshotsRepoUrl = uri("$buildDir/repos/snapshots")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

repositories {
    jcenter()
}

dependencies {
    val jupiterVersion = "5.6.2"
    val kafkaClientVersion = "2.7.0"
    val slf4jVersion = "1.7.30"
    val assertjVersion = "3.18.1"
    val testContainersVersion = "1.15.1"

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    api("org.jetbrains.kotlin:kotlin-stdlib-jdk8")

    api("org.apache.kafka:kafka-clients:$kafkaClientVersion")
    runtimeOnly("org.slf4j:slf4j-simple:$slf4jVersion")

    implementation(platform("org.testcontainers:testcontainers-bom:$testContainersVersion"))
    api("org.testcontainers:kafka")

    api("org.junit.jupiter:junit-jupiter-api:$jupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-engine:$jupiterVersion")
    testImplementation("org.junit.jupiter:junit-jupiter-params:$jupiterVersion")
    testImplementation("org.assertj:assertj-core:$assertjVersion")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging.events("passed", "skipped")
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "1.8"

        freeCompilerArgs = listOf("-Xjvm-default=enable")
    }
}
