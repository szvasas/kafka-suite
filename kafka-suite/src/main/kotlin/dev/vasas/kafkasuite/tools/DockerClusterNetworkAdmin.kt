@file:JvmName("DockerClusterNetworkAdmin")

package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.DockerKafkaCluster
import java.time.Duration

fun DockerKafkaCluster.setNetworkDelay(nodeId: Int, delay: Duration) {
    val command = "tc qdisc add dev eth0 root netem delay ${delay.toMillis()}ms".split(" ")
    executeOnKafkaNode(nodeId, command)
}

fun DockerKafkaCluster.enableNetwork(nodeId: Int) {
    val command = "ip link set eth0 up".split(" ")
    executeOnKafkaNode(nodeId, command)
}

fun DockerKafkaCluster.disableNetwork(nodeId: Int) {
    val command = "ip link set eth0 down".split(" ")
    executeOnKafkaNode(nodeId, command)
}
