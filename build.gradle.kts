plugins {
    id("org.jetbrains.kotlin.jvm") version "1.4.21"
}

repositories {
    jcenter()
}

dependencies {
    val jupiterVersion = "5.6.2"
    val kafkaVersion = "2.6.0"
    val slf4jVersion = "1.7.30"
    val assertjVersion = "3.18.1"
    val testContainersVersion = "1.15.1"

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("org.slf4j:slf4j-simple:$slf4jVersion")
    implementation(platform("org.testcontainers:testcontainers-bom:$testContainersVersion"))
    implementation("org.testcontainers:kafka")

    implementation("org.junit.jupiter:junit-jupiter-api:$jupiterVersion")
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
