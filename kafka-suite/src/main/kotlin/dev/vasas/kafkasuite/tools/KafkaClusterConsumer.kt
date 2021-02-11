@file:JvmName("KafkaClusterConsumer")

package dev.vasas.kafkasuite.tools

import dev.vasas.kafkasuite.cluster.KafkaCluster
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

fun KafkaCluster.consumeAllTestRecordsFromTopic(
        topic: String
): List<TestRecord<String, String>> {
    return consumeAllRecordsFromTopic(topic, StringDeserializer::class.java, StringDeserializer::class.java).map {
        it.toTestRecord()
    }
}

fun KafkaCluster.consumeAllRecordsFromTopic(
        topic: String
): List<ConsumerRecord<String, String>> {
    return consumeAllRecordsFromTopic(topic, StringDeserializer::class.java, StringDeserializer::class.java)
}

fun <K, V> KafkaCluster.consumeAllRecordsFromTopic(
        topic: String,
        keyDeserializer: Class<out Deserializer<K>>,
        valueDeserializer: Class<out Deserializer<V>>,
): List<ConsumerRecord<K, V>> {

    kafkaConsumer(keyDeserializer, valueDeserializer).use { consumer ->
        val topicPartitions = consumer.partitionsFor(topic).map {
            TopicPartition(it.topic(), it.partition())
        }

        consumer.assign(topicPartitions)
        consumer.seekToBeginning(topicPartitions)

        val result: MutableList<ConsumerRecord<K, V>> = mutableListOf()

        do {
            val batch = consumer.poll(Duration.ofSeconds(1L))
            result.addAll(batch)
        } while (!batch.isEmpty)

        return result
    }
}

private fun <K, V> KafkaCluster.kafkaConsumer(
        keyDeserializer: Class<out Deserializer<K>>,
        valueDeserializer: Class<out Deserializer<V>>
): Consumer<K, V> {
    return KafkaConsumer(mapOf(
            BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            KEY_DESERIALIZER_CLASS_CONFIG to keyDeserializer.name,
            VALUE_DESERIALIZER_CLASS_CONFIG to valueDeserializer.name
    ))
}
