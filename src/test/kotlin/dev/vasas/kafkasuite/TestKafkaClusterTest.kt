package dev.vasas.kafkasuite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


class TestKafkaClusterTest {

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun `buildKafkaCluster creates cluster with correct number of Kafka nodes`(clusterSize: Int) {

        assertThat(buildTestKafkaCluster(clusterSize).kafkaNodes.size).isEqualTo(clusterSize)
    }

    @Test
    fun `a 3 node Kafka cluster is initialized and started`() {
        val testKafkaCluster = buildTestKafkaCluster()
        testKafkaCluster.startCluster()

        assertThat(testKafkaCluster.isRunning()).isTrue
    }
}
