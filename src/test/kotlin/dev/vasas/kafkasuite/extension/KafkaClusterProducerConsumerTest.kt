package dev.vasas.kafkasuite.extension

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.extension.producer.createStringProducer
import dev.vasas.kafkasuite.junit5.KafkaSuite
import org.apache.kafka.clients.producer.ProducerRecord
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
    inner class `when messages are produced on a topic` {

        val testTopic = UUID.randomUUID().toString()
        val testMessages = listOf("message1", "message2", "message3")

        @BeforeAll
        fun beforeAll() {
            kafkaCluster.createStringProducer().use { producer ->
                testMessages.forEach {
                    producer.send(ProducerRecord(testTopic, it))
                }
            }
        }

        @Test
        fun `consumer can read all of them`() {
            val consumedMessages = kafkaCluster.consumeAllRecordsFromTopic(testTopic).map {
                it.value()
            }

            assertThat(consumedMessages).containsExactlyInAnyOrderElementsOf(testMessages)
        }
    }

}
