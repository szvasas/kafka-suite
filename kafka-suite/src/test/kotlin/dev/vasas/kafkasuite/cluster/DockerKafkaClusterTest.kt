package dev.vasas.kafkasuite.cluster

import io.kotest.matchers.ints.shouldBeExactly
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class DockerKafkaClusterTest {

    @ParameterizedTest
    @ValueSource(ints = [1, 2, 3, 4, 5])
    fun `createDockerKafkaCluster creates cluster with correct number of Kafka nodes`(clusterSize: Int) {
        createDockerKafkaCluster(clusterSize).size shouldBeExactly clusterSize
    }
}
