package dev.vasas.kafkasuite

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName.parse
import java.util.concurrent.CompletableFuture

typealias ZookeeperContainer = GenericContainer<Nothing>

private const val DEFAULT_ZOOKEEPER_IMAGE = "zookeeper:3.4.9"
private const val DEFAULT_KAFKA_IMAGE = "confluentinc/cp-kafka:5.4.3"
private const val ZOOKEEPER_NETWORK_ALIAS = "zookeeper"

class KafkaCluster(private val zookeeperNode: ZookeeperContainer,
                   val kafkaNodes: List<KafkaContainer>) {

    val isRunning: Boolean
        get() {
            return zookeeperNode.isRunning && kafkaNodes.all(KafkaContainer::isRunning)
        }

    val bootstrapServers: String
        get() {
            return kafkaNodes.joinToString(",", transform = KafkaContainer::getBootstrapServers)
        }

    fun start() {
        zookeeperNode.start()

        val launchingJobs = kafkaNodes.map {
            CompletableFuture.runAsync {
                it.start()
            }
        }
        CompletableFuture.allOf(*launchingJobs.toTypedArray()).get()
    }

    fun stop() {
        zookeeperNode.stop()
        kafkaNodes.forEach(KafkaContainer::stop)
    }
}

fun buildKafkaCluster(nodeCount: Int = 3,
                      kafkaImage: String = DEFAULT_KAFKA_IMAGE,
                      zookeeperImage: String = DEFAULT_ZOOKEEPER_IMAGE
): KafkaCluster {
    val network = Network.newNetwork()
    val zookeeperNode = createZookeeperNode(zookeeperImage, network)

    val kafkaNodes = (1..nodeCount).map { nodeId ->
        createKafkaNode(kafkaImage, network, nodeId)
    }

    return KafkaCluster(zookeeperNode, kafkaNodes)
}

private fun createZookeeperNode(zookeeperImage: String, network: Network): ZookeeperContainer {
    return ZookeeperContainer(parse(zookeeperImage)).apply {
        withNetwork(network)
        withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
    }
}

private fun createKafkaNode(kafkaImage: String, network: Network, id: Int): KafkaContainer {
    return KafkaContainer(parse(kafkaImage))
            .withNetwork(network)
            .withExternalZookeeper("$ZOOKEEPER_NETWORK_ALIAS:2181")
            .withEnv("KAFKA_BROKER_ID", id.toString())
}
