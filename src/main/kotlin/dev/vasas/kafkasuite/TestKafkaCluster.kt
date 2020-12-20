package dev.vasas.kafkasuite

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName.parse

typealias ZookeeperContainer = GenericContainer<Nothing>

private const val DEFAULT_ZOOKEEPER_IMAGE = "zookeeper:3.4.9"
private const val DEFAULT_KAFKA_IMAGE = "confluentinc/cp-kafka:5.4.3"
private const val ZOOKEEPER_NETWORK_ALIAS = "zookeeper"

class TestKafkaCluster(val zookeeperNode: ZookeeperContainer,
                       val kafkaNodes: List<KafkaContainer>) {

    fun startCluster() {
        zookeeperNode.start()

        runBlocking {
            val launchingJobs = kafkaNodes.map {
                GlobalScope.launch {
                    it.start()
                }
            }
            launchingJobs.joinAll()
        }
    }

    fun stopCluster() {
        zookeeperNode.stop()
        kafkaNodes.forEach(KafkaContainer::stop)
    }

    fun isRunning(): Boolean {
        return zookeeperNode.isRunning && kafkaNodes.all(KafkaContainer::isRunning)
    }

}

fun buildTestKafkaCluster(nodeCount: Int = 3,
                          kafkaImage: String = DEFAULT_KAFKA_IMAGE,
                          zookeeperImage: String = DEFAULT_ZOOKEEPER_IMAGE
): TestKafkaCluster {
    val network = Network.newNetwork()
    val zookeeperNode = createZookeeperContainer(zookeeperImage, network)

    val kafkaNodes = (1..nodeCount).map { nodeId ->
        createKafkaContainer(kafkaImage, network, nodeId)
    }

    return TestKafkaCluster(zookeeperNode, kafkaNodes)
}

private fun createZookeeperContainer(zookeeperImage: String, network: Network): ZookeeperContainer {
    return ZookeeperContainer(parse(zookeeperImage)).apply {
        withNetwork(network)
        withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
    }
}

private fun createKafkaContainer(kafkaImage: String, network: Network, id: Int): KafkaContainer {
    return KafkaContainer(parse(kafkaImage))
            .withNetwork(network)
            .withExternalZookeeper("$ZOOKEEPER_NETWORK_ALIAS:2181")
            .withEnv("KAFKA_BROKER_ID", id.toString())
}
