@file:JvmName("KafkaClusterConsumer")

package dev.vasas.kafkasuite.extension

import dev.vasas.kafkasuite.cluster.KafkaCluster
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer

@JvmOverloads
fun <K, V> KafkaCluster.createProducer(config: Map<String, String> = emptyMap()): Producer<K, V> {
    val default = defaultConfig(bootstrapServers)
    (default.keys + config.keys).associateWith {
        key -> config.getOrDefault(key, default[key])
    }
    return KafkaProducer(config)
}

@JvmOverloads
fun KafkaCluster.createStringProducer(config: Map<String, String> = emptyMap()): Producer<String, String> {
    return createProducer(config)
}

private fun defaultConfig(bootstrapServers: String): Map<String, String> {
    return mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java.name,
            ProducerConfig.ACKS_CONFIG to "all",
            ProducerConfig.RETRIES_CONFIG to "3"
    )
}
