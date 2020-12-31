package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.producer.createStringProducer
import dev.vasas.kafkasuite.junit5.KafkaSuite
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.*

class KafkaClusterProducerConsumerTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster()

    @Test
    fun `consumer of a non-existing topic returns an empty list`() {
        val testTopic = UUID.randomUUID().toString()
        val allRecords = kafkaCluster.consumeAllRecordsFromTopic(testTopic)
        assertThat(allRecords).isEmpty()
    }

    @Nested
    inner class `given some messages are are available on a topic` {

        val testTopic = UUID.randomUUID().toString()
        val testMessages = generateStringRecords(testTopic, num = 3)

        @BeforeAll
        fun beforeAll() {
            kafkaCluster.createStringProducer().use { producer ->
                testMessages.forEach {
                    producer.send(it.toProducerRecord())
                }
            }
        }

        @Test
        fun `consumer can read all of them`() {
            val consumedMessages = kafkaCluster.consumeAllTestRecordsFromTopic(testTopic)

            assertThat(consumedMessages).containsExactlyInAnyOrderElementsOf(testMessages)
        }
    }

}
