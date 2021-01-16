package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.tools.disableNetwork
import dev.vasas.kafkasuite.tools.enableNetwork
import dev.vasas.kafkasuite.tools.generateStringRecords
import kotlinx.coroutines.delay
import org.apache.kafka.clients.producer.ProducerConfig
import java.time.Duration

fun main() {

    val scenario = Scenario(
            kafkaCluster = createDockerKafkaCluster(1),
            topics = listOf(
                    Topic()
            ),
            producers = listOf(
                    Producer(
                            recordGenerator = { topics ->
                                generateStringRecords(topics.first().name)
                            },
                            rate = 50L,
                            config = mapOf(
                                    ProducerConfig.ACKS_CONFIG to "1"
                            )
                    )
            ),
            actions = listOf(
                    Action(
                            trigger = {
                                it.metrics.sent > 150
                            },
                            action = {
                                println("Disable network")
                                it.kafkaCluster.disableNetwork(0)

                                delay(Duration.ofSeconds(130).toMillis())
                                println("Enable network")
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
