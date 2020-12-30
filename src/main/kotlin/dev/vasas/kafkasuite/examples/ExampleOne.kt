package dev.vasas.kafkasuite.examples

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.createTopic
import dev.vasas.kafkasuite.tools.producer.Metrics
import dev.vasas.kafkasuite.tools.producer.createStringProducer
import dev.vasas.kafkasuite.tools.producer.withMetricsDecorator
import dev.vasas.kafkasuite.tools.producer.withSendRateDecorator
import dev.vasas.kafkasuite.tools.setNetworkDelay
import dev.vasas.kafkasuite.tools.stringRecordSequence
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    val testTopic = "testTopic"
    val testTopicNumPartitions = 1
    val testTopicReplicationFactor: Short = 1

    val kafkaCluster = createDockerKafkaCluster()
    kafkaCluster.start()

    kafkaCluster.createTopic(testTopic, testTopicNumPartitions, testTopicReplicationFactor)
    kafkaCluster.setNetworkDelay(0, Duration.ofMillis(100L))

    val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()

    val producerSwitch = AtomicBoolean(true)
    CompletableFuture.runAsync {
        kafkaCluster.createStringProducer()
                .withMetricsDecorator(metricsQueue)
                .withSendRateDecorator(50L)
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
        var metrics = Metrics<String, String>()
        while (producerSwitch.get()) {
            metrics = metrics.aggregate(metricsQueue)
            println(metrics)
            Thread.sleep(1000L)
        }
        metrics = metrics.aggregate(metricsQueue)
        println(metrics)
    }

    Thread.sleep(15000L)

    producerSwitch.set(false)

    Thread.sleep(1000L)
}
