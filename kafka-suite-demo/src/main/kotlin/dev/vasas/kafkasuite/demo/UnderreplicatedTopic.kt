package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.disableNetwork
import dev.vasas.kafkasuite.tools.enableNetwork
import dev.vasas.kafkasuite.tools.generateStringRecords
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.ProducerConfig
import java.time.Duration

fun main() {

    val deliveryTimeout = Duration.ofSeconds(30)

    val scenario = Scenario(
            kafkaCluster = createDockerKafkaCluster(3),
            topics = listOf(
                    Topic(numPartitions = 1, replicationFactor = 3, minInSyncReplicas = 3)
            ),
            producers = listOf(
                    Producer(
                            recordGenerator = { topics ->
                                generateStringRecords(topics.first().name)
                            },
                            rate = 50L,
                            config = mapOf(
                                    ProducerConfig.ACKS_CONFIG to "all",
                                    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG to deliveryTimeout.toMillis().toInt()
                            )
                    )
            ),
            actions = listOf(
                    Action(
                            trigger = {
                                it.metrics.sent > 150
                            },
                            action = {
                                println("Disabling network on node 0")
                                it.kafkaCluster.disableNetwork(0)

                                delay((deliveryTimeout + Duration.ofSeconds(10)).toMillis())
                                println("Enabling network on node 0")
                                it.kafkaCluster.enableNetwork(0)
                            }
                    ),
                    Action(
                            trigger = {
                                it.metrics.sent > 200
                            },
                            action = {
                                println("Stopping producer.")
                                it.producerSwitch.set(false)
                            }
                    )
            )
    )

    scenario.run()

}
