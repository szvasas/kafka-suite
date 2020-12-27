package dev.vasas.kafkasuite.cluster

import dev.vasas.kafkasuite.createTopic
import dev.vasas.kafkasuite.deleteTopic
import dev.vasas.kafkasuite.listNodes
import dev.vasas.kafkasuite.listTopics
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class DockerKafkaClusterTest {

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun `createDockerKafkaCluster creates cluster with correct number of Kafka nodes`(clusterSize: Int) {
        assertThat(createDockerKafkaCluster(clusterSize).size).isEqualTo(clusterSize)
    }

    @Nested
    inner class `when a 3 node Kafka cluster is started` {

        lateinit var kafkaCluster: DockerKafkaCluster

        @BeforeAll
        fun beforeAll() {
            kafkaCluster = createDockerKafkaCluster()
            kafkaCluster.start()
        }

        @AfterAll
        fun afterAll() {
            kafkaCluster.stop()
        }

        @Test
        fun `isRunning is true`() {
            assertThat(kafkaCluster.isRunning).isTrue
        }

        @Test
        fun `nodes with proper IDs are available`() {
            val nodeIds = kafkaCluster.listNodes().map {
                it.id()
            }
            assertThat(nodeIds).containsExactlyInAnyOrderElementsOf(1..kafkaCluster.size)
        }

        @Test
        fun `there are no topics`() {
            assertThat(kafkaCluster.listTopics()).isEmpty()
        }

        @Nested
        inner class `and a new topic is created` {

            val topicName = "myTestTopic"
            val numPartitions = 2
            val replicationFactor = 3

            @BeforeAll
            fun beforeAll() {
                kafkaCluster.createTopic(topicName, numPartitions, replicationFactor.toShort())
            }

            @AfterAll
            fun afterAll() {
                kafkaCluster.deleteTopic(topicName)
            }

            @Test
            fun `the topic is available in the cluster with correct parameters`() {
                val topics = kafkaCluster.listTopics().filterNot { it.name().startsWith("__confluent") }

                assertSoftly { softly ->
                    softly.assertThat(topics.size).isEqualTo(1)

                    val topic = topics.first()
                    softly.assertThat(topic.name()).isEqualTo(topicName)
                    softly.assertThat(topic.partitions().size).isEqualTo(numPartitions)

                    topic.partitions().forEach {
                        softly.assertThat(it.replicas().size).isEqualTo(replicationFactor)
                    }

                    softly.assertAll()
                }
            }
        }
    }
}
