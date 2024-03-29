plugins {
    id("org.jetbrains.kotlin.jvm")
    id("application")
}
application.mainClassName = "dev.vasas.kafkasuite.demo.StartupClusterKt"
dependencies {
    val kotlinxVersion = "1.6.4"

    implementation(project(":kafka-suite"))

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:$kotlinxVersion"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:${project.ext["jupiterVersion"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${project.ext["jupiterVersion"]}")
    testImplementation("io.kotest:kotest-assertions-core:${project.ext["kotestVersion"]}")
}
