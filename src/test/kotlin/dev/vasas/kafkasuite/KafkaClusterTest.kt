package dev.vasas.kafkasuite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource


class KafkaClusterTest {

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun `buildKafkaCluster creates cluster with correct number of Kafka nodes`(clusterSize: Int) {
        assertThat(buildKafkaCluster(clusterSize).kafkaNodes.size).isEqualTo(clusterSize)
    }

    @Test
    fun `a 3 node Kafka cluster is initialized and started`() {
        val testKafkaCluster = buildKafkaCluster()
        testKafkaCluster.startCluster()

        assertThat(testKafkaCluster.isRunning()).isTrue

        testKafkaCluster.stopCluster()
    }
}
