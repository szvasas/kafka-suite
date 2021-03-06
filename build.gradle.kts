plugins {
    id("org.jetbrains.kotlin.jvm") version "1.4.21" apply false
}

allprojects {
    repositories {
        jcenter()
    }
}

subprojects {
    group = "dev.vasas"
    version = "1.0.SNAPSHOT"

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

    ext {
        set("jupiterVersion", "5.6.2")
        set("kotestVersion", "4.4.0")
    }

}
