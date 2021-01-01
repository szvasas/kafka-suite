package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class KafkaClusterAdminTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster(nodeCount = 3)

    @Test
    fun `listNodes() returns all the Kafka nodes`() {
        val nodeIds = kafkaCluster.listNodes().map {
            it.id()
        }
        assertThat(nodeIds).containsExactlyInAnyOrderElementsOf(0 until kafkaCluster.size)
    }

    @Test
    fun `listTopics() returns an empty list`() {
        assertThat(kafkaCluster.listTopics()).isEmpty()
    }

    @Nested
    inner class `when a new topic is created` {

        val topicName = "myTestTopic"
        val numPartitions = 2
        val replicationFactor = 3

        @BeforeAll
        fun beforeAll() {
            kafkaCluster.createTopic(topicName, numPartitions, replicationFactor.toShort())
        }

        @Test
        fun `the topic is available in the cluster with correct parameters`() {
            val createdTopic = kafkaCluster.listTopics().first()

            assertSoftly { softly ->
                softly.assertThat(createdTopic.name()).isEqualTo(topicName)
                softly.assertThat(createdTopic.partitions().size).isEqualTo(numPartitions)

                createdTopic.partitions().forEach {
                    softly.assertThat(it.replicas().size).isEqualTo(replicationFactor)
                }

                softly.assertAll()
            }
        }

        @Nested
        inner class `when the previously created topic is deleted` {

            @BeforeAll
            fun beforeAll() {
                kafkaCluster.deleteTopic(topicName)
            }

            @Test
            fun `listTopics() returns empty an list again`() {
                assertThat(kafkaCluster.listTopics()).isEmpty()
            }
        }

    }
}
