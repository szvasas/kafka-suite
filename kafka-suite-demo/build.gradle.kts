plugins {
    id("org.jetbrains.kotlin.jvm")
}

dependencies {
    implementation(project(":kafka-suite"))

    testImplementation("org.junit.jupiter:junit-jupiter-engine:${project.ext["jupiterVersion"]}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${project.ext["jupiterVersion"]}")
    testImplementation("org.assertj:assertj-core:${project.ext["assertjVersion"]}")
}
