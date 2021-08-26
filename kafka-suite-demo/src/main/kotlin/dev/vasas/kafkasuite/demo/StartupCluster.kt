package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.createTopic

fun main() {
    val kafkaCluster = createDockerKafkaCluster(3)
    kafkaCluster.start()
    kafkaCluster.createTopic(
            topicName = "mytopic",
            numPartitions = 1,
            replicationFactor = 3,
            minInSyncReplicas = 3
    )

    Thread.sleep(Long.MAX_VALUE)
}
