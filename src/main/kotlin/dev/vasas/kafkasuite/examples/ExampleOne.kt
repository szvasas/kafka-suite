package dev.vasas.kafkasuite.examples

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.createTopic
import dev.vasas.kafkasuite.tools.producer.Metrics
import dev.vasas.kafkasuite.tools.producer.createStringProducer
import dev.vasas.kafkasuite.tools.producer.withMetricsDecorator
import dev.vasas.kafkasuite.tools.producer.withSendRateDecorator
import dev.vasas.kafkasuite.tools.stringRecordSequence
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    val testTopic = "testTopic"
    val testTopicNumPartitions = 1
    val testTopicReplicationFactor: Short = 1

    val kafkaCluster = createDockerKafkaCluster()
    kafkaCluster.start()

    kafkaCluster.createTopic(testTopic, testTopicNumPartitions, testTopicReplicationFactor)

    val metrics = Metrics<String, String>()

    val producerSwitch = AtomicBoolean(true)
    CompletableFuture.runAsync {
        kafkaCluster.createStringProducer()
                .withSendRateDecorator(100L)
                .withMetricsDecorator(metrics)
                .use { producer ->
                    stringRecordSequence(testTopic).forEach { record ->
                        producer.send(record)
                        if (!producerSwitch.get()) {
                            return@use
                        }
                    }
                }
    }

    CompletableFuture.runAsync {
        while (true) {
            println(metrics)
            Thread.sleep(1000L)
        }
    }

    Thread.sleep(15000L)

    producerSwitch.set(false)


    Thread.sleep(15000L)
}
