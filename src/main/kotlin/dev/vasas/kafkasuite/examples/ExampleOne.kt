package dev.vasas.kafkasuite.examples

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.createTopic
import dev.vasas.kafkasuite.tools.producer.Metrics
import dev.vasas.kafkasuite.tools.producer.aggregate
import dev.vasas.kafkasuite.tools.producer.createStringProducer
import dev.vasas.kafkasuite.tools.producer.withMetricsDecorator
import dev.vasas.kafkasuite.tools.producer.withSendRateDecorator
import dev.vasas.kafkasuite.tools.stringRecordSequence
import dev.vasas.kafkasuite.tools.toProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    val testTopic = "testTopic"
    val testTopicNumPartitions = 1
    val testTopicReplicationFactor: Short = 1

    val kafkaCluster = createDockerKafkaCluster()
    kafkaCluster.start()

    kafkaCluster.createTopic(testTopic, testTopicNumPartitions, testTopicReplicationFactor)

    val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()

    val producerSwitch = AtomicBoolean(true)
    CompletableFuture.runAsync {
        val producerConfig = mapOf(
                ProducerConfig.ACKS_CONFIG to "1"
        )
        kafkaCluster.createStringProducer(producerConfig)
                .withMetricsDecorator(metricsQueue)
                .withSendRateDecorator(50L)
                .use { producer ->
                    stringRecordSequence(testTopic).forEach { record ->
                        producer.send(record.toProducerRecord())
                        if (!producerSwitch.get()) {
                            return@use
                        }
                    }
                }
    }

    var totalMetrics = Metrics<String, String>()
    var runningMetrics = Metrics<String, String>()
    var firstAction = true
    var secondAction = true
    while (producerSwitch.get()) {
        val increment = metricsQueue.aggregate()
        runningMetrics += increment
        totalMetrics += increment
        println(runningMetrics)
        println()
        Thread.sleep(1000L)

        if (totalMetrics.sent >= 150 && firstAction) {
            kafkaCluster.stopKafkaNode(0)
            firstAction = false

            val delayedExecutor = CompletableFuture.delayedExecutor(15, TimeUnit.SECONDS)
            CompletableFuture.runAsync({
                println("Starting the cluster")
                kafkaCluster.startKafkaNode(0)
            }, delayedExecutor)
        }

        if (totalMetrics.sent >= 185 && secondAction) {
            producerSwitch.set(false)
            secondAction = false
        }
    }

    println(totalMetrics)

}
