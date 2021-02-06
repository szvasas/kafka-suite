package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.ints.shouldBeExactly
import io.kotest.matchers.shouldBe
import org.apache.kafka.common.Node
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

class KafkaClusterAdminTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster(nodeCount = 3)

    @Test
    fun `listNodes() returns all the Kafka nodes`() {
        val nodeIds = kafkaCluster.listNodes().map(Node::id)

        nodeIds shouldContainExactly (0 until kafkaCluster.size)
    }

    @Test
    fun `listTopics() returns an empty list`() {
        kafkaCluster.listTopics().isEmpty() shouldBe true
    }

    @Nested
    inner class `when a new topic is created` {

        private val topicName = "myTestTopic"
        private val numPartitions = 2
        private val replicationFactor = 3

        @BeforeAll
        fun beforeAll() {
            kafkaCluster.createTopic(topicName, numPartitions, replicationFactor.toShort())
        }

        @Test
        fun `the topic is available in the cluster with correct parameters`() {
            val createdTopic = kafkaCluster.listTopics().first()

            assertSoftly {
                createdTopic.name() shouldBe topicName
                createdTopic.partitions().size shouldBeExactly numPartitions

                createdTopic.partitions().forEach {
                    it.replicas().size shouldBeExactly replicationFactor
                }
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
                kafkaCluster.listTopics().isEmpty() shouldBe true
            }
        }

    }
}
