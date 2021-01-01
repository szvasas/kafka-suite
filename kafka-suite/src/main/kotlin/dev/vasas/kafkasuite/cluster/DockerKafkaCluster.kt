@file:JvmName("DockerKafkaClusterFactory")

package dev.vasas.kafkasuite.cluster

import com.github.dockerjava.api.model.Capability
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName
import java.util.concurrent.CompletableFuture

typealias ZookeeperContainer = GenericContainer<Nothing>

private const val DEFAULT_CLUSTER_SIZE = 1
private const val DEFAULT_ZOOKEEPER_IMAGE = "zookeeper:3.4.9"
private const val DEFAULT_KAFKA_IMAGE = "confluentinc/cp-kafka:5.4.3"
private const val ZOOKEEPER_NETWORK_ALIAS = "zookeeper"

class DockerKafkaCluster(
        private val zookeeperNode: ZookeeperContainer,
        private val kafkaNodes: List<KafkaContainer>
) : KafkaCluster {

    val size: Int = kafkaNodes.size

    override val bootstrapServers: String
        get() {
            return kafkaNodes.filter {
                it.isRunning
            }.joinToString(",", transform = KafkaContainer::getBootstrapServers)
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
        kafkaNodes.forEach(KafkaContainer::stop)
        zookeeperNode.stop()
    }

    fun startZookeeperNode() {
        zookeeperNode.start()
    }

    fun startKafkaNode(nodeId: Int) {
        checkNodeId(nodeId)
        kafkaNodes[nodeId].start()
    }

    fun stopZookeeper() {
        zookeeperNode.start()
    }

    fun stopKafkaNode(nodeId: Int) {
        checkNodeId(nodeId)
        kafkaNodes[nodeId].stop()
    }

    fun executeOnKafkaNode(nodeId: Int, commands: List<String>): ExecResult {
        checkNodeId(nodeId)
        val result = kafkaNodes[nodeId].execInContainer(*commands.toTypedArray())
        return ExecResult(result.exitCode, result.stdout, result.stderr)
    }

    fun pauseKafkaNode(nodeId: Int) {
        checkNodeId(nodeId)
        kafkaNodes[nodeId].dockerClient.pauseContainerCmd(kafkaNodes[nodeId].containerId).exec()
    }

    fun unpauseKafkaNode(nodeId: Int) {
        checkNodeId(nodeId)
        kafkaNodes[nodeId].dockerClient.unpauseContainerCmd(kafkaNodes[nodeId].containerId).exec()
    }

    private fun checkNodeId(nodeId: Int) {
        require(nodeId in 0 until size) {
            "Node Id must be in range [0, clusterSize)"
        }
    }

}

data class ExecResult(val exitCode: Int, val stdOut: String, val stdErr: String)

@JvmOverloads
fun createDockerKafkaCluster(
        nodeCount: Int = DEFAULT_CLUSTER_SIZE,
        kafkaImage: String = DEFAULT_KAFKA_IMAGE,
        zookeeperImage: String = DEFAULT_ZOOKEEPER_IMAGE
): DockerKafkaCluster {
    val network = Network.newNetwork()
    val zookeeperNode = createZookeeperNode(zookeeperImage, network)

    val kafkaNodes = (0 until nodeCount).map { nodeId ->
        createKafkaNode(kafkaImage, network, nodeId)
    }

    return DockerKafkaCluster(zookeeperNode, kafkaNodes)
}

private fun createZookeeperNode(zookeeperImage: String, network: Network): ZookeeperContainer {
    return ZookeeperContainer(DockerImageName.parse(zookeeperImage)).apply {
        withNetwork(network)
        withNetworkAliases(ZOOKEEPER_NETWORK_ALIAS)
    }
}

private fun createKafkaNode(kafkaImage: String, network: Network, id: Int): KafkaContainer {
    return KafkaContainer(DockerImageName.parse(kafkaImage))
            .withNetwork(network)
            .withExternalZookeeper("$ZOOKEEPER_NETWORK_ALIAS:2181")
            .withEnv("KAFKA_BROKER_ID", id.toString())
            .withEnv("CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
            .withCreateContainerCmdModifier {
                it.hostConfig?.withCapAdd(Capability.NET_ADMIN)
            }
}
