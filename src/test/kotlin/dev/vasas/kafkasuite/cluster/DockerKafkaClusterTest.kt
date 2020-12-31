package dev.vasas.kafkasuite.cluster

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class DockerKafkaClusterTest {

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun `createDockerKafkaCluster creates cluster with correct number of Kafka nodes`(clusterSize: Int) {
        assertThat(createDockerKafkaCluster(clusterSize).size).isEqualTo(clusterSize)
    }
}
