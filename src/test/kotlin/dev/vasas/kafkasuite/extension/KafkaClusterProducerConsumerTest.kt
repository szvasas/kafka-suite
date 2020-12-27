package dev.vasas.kafkasuite.extension

import dev.vasas.kafkasuite.cluster.DockerKafkaCluster
import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.extension.producer.consumeAllRecordsFromTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.*

class KafkaClusterProducerConsumerTest {

    lateinit var kafkaCluster: DockerKafkaCluster

    @BeforeAll
    fun beforeAll() {
        kafkaCluster = createDockerKafkaCluster(nodeCount = 1)
        kafkaCluster.start()
    }

    @AfterAll
    fun afterAll() {
        kafkaCluster.stop()
    }

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
