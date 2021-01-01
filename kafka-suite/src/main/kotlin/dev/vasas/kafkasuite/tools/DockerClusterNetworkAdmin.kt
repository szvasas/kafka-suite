@file:JvmName("DockerClusterNetworkAdmin")

package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.DockerKafkaCluster
import java.time.Duration

fun DockerKafkaCluster.setNetworkDelay(nodeId: Int, delay: Duration) {
    val command = "tc qdisc add dev eth0 root netem delay ${delay.toMillis()}ms".split(" ")
    executeOnKafkaNode(nodeId, command)
}
