/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */
package dev.vasas.kafkasuite

import org.assertj.core.api.Assertions.assertThat
import org.junit.Test


class AppTest {

    @Test
    fun testAppHasAGreeting() {
        val classUnderTest = App()
        assertThat(classUnderTest.greeting).isNotNull
    }
}
