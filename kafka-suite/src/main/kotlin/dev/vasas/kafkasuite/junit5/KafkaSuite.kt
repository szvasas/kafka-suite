package dev.vasas.kafkasuite.junit5

import dev.vasas.kafkasuite.cluster.DockerKafkaCluster
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
interface KafkaSuite {

    val kafkaCluster: DockerKafkaCluster

    @BeforeAll
    @JvmDefault
    fun kafkaSuiteBeforeAll() {
        kafkaCluster.start()
    }

    @AfterAll
    @JvmDefault
    fun kafkaSuiteAfterAll() {
        kafkaCluster.stop()
    }

}
