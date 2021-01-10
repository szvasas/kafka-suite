plugins {
    id("org.jetbrains.kotlin.jvm")
}

dependencies {
    val kotlinxVersion = "1.4.2"

    implementation(project(":kafka-suite"))

    implementation(platform("org.jetbrains.kotlin:kotlin-bom"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation(platform("org.jetbrains.kotlinx:kotlinx-coroutines-bom:$kotlinxVersion"))
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8")

    testImplementation("org.junit.jupiter:junit-jupiter-engine:${project.ext["jupiterVersion"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${project.ext["jupiterVersion"]}")
    testImplementation("org.assertj:assertj-core:${project.ext["assertjVersion"]}")
}
