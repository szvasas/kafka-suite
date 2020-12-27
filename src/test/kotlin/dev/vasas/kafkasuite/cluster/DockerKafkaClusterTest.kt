package dev.vasas.kafkasuite.cluster

import org.assertj.core.api.Assertions.assertThat
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

    }
}
