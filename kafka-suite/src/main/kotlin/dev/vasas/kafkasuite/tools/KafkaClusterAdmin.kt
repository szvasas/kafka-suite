@file:JvmName("KafkaClusterAdmin")

package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.KafkaCluster
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.common.Node


@JvmOverloads
fun KafkaCluster.createTopic(topicName: String, numPartitions: Int = 1, replicationFactor: Short = 1, minInSyncReplicas: Short = 1) {
    adminClient().use { admin ->
        val newTopic = NewTopic(topicName, numPartitions, replicationFactor).apply {
            configs(mapOf("min.insync.replicas" to minInSyncReplicas.toString()))
        }
        admin.createTopics(listOf(newTopic)).all().get()
    }

    verifyWithAllNodes { admin ->
        admin.listTopicNames().contains(topicName)
    }
}

fun KafkaCluster.deleteTopic(topicName: String) {
    adminClient().use {
        it.deleteTopics(listOf(topicName)).all().get()
    }

    verifyWithAllNodes { admin ->
        admin.listTopicNames().contains(topicName)
    }
}

fun KafkaCluster.listNodes(): Collection<Node> {
    adminClient().use {
        return it.describeCluster().nodes().get()
    }
}

fun KafkaCluster.listTopics(): Collection<TopicDescription> {
    adminClient().use { admin ->
        val topicNames = admin.listTopicNames()
        return admin.describeTopics(topicNames).all().get().values
    }
}

private inline fun KafkaCluster.verifyWithAllNodes(block: (Admin) -> Boolean) {
    bootstrapServers.split(",").forEach { server ->
        waitUntil {
            adminClient(server).use { admin ->
                block(admin)
            }
        }
    }
}

private inline fun waitUntil(block: () -> Boolean) {
    val maxRetries = 20
    var retry = 0
    while (!block() && ++retry < maxRetries) {
        Thread.sleep(100L)
    }
}

private fun Admin.listTopicNames(): List<String> {
    return listTopics().listings().get().map { it.name() }
}

private fun KafkaCluster.adminClient(): Admin {
    return AdminClient.create(mapOf(
            BOOTSTRAP_SERVERS_CONFIG to bootstrapServers
    ))
}

private fun adminClient(bootstrapServers: String): Admin {
    return AdminClient.create(mapOf(
            BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
    ))
}
