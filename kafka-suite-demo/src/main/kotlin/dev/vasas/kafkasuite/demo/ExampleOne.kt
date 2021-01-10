package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.createStringProducer
import dev.vasas.kafkasuite.tools.createTopic
import dev.vasas.kafkasuite.tools.disableNetwork
import dev.vasas.kafkasuite.tools.enableNetwork
import dev.vasas.kafkasuite.tools.generateStringRecords
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerConfig
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

fun main() {
    val testTopic = "testTopic"
    val testTopicNumPartitions = 1
    val testTopicReplicationFactor: Short = 1

    val kafkaCluster = createDockerKafkaCluster()
    kafkaCluster.start()

    kafkaCluster.createTopic(testTopic, testTopicNumPartitions, testTopicReplicationFactor)

    val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()

    val isActive = AtomicBoolean(true)

    val producerConfig = mapOf(
            ProducerConfig.ACKS_CONFIG to "1"
    )
    val producer = Producer(
            id = "firstProducer",
            kafkaProducer = kafkaCluster.createStringProducer(producerConfig),
            records = generateStringRecords(testTopic),
            rate = 50L,
            metricsQueue = metricsQueue,
            isActive = isActive
    )

    GlobalScope.launch {
        producer.produce()
    }

    runBlocking {
        var totalMetrics = Metrics<String, String>()
        var firstAction = true
        var secondAction = true
        while (isActive.get()) {
            val increment = metricsQueue.aggregate()
            totalMetrics += increment
            println(totalMetrics)
            println()
            delay(1000L)

            if (totalMetrics.sent >= 150 && firstAction) {
                kafkaCluster.disableNetwork(0)
                firstAction = false

                GlobalScope.launch {
                    delay(Duration.ofSeconds(130).toMillis())
                    println("Starting the cluster")
                    kafkaCluster.enableNetwork(0)
                }
            }

            if (totalMetrics.sent >= 200 && secondAction) {
                isActive.set(false)
                secondAction = false
            }
        }

        println(totalMetrics)

    }

}
