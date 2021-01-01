@file:JvmName("TestDataGenerator")

package dev.vasas.kafkasuite.tools

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

data class TestRecord<K, V>(val topic: String, val key: K?, val value: V)

fun generateStringRecords(topic: String, num: Int): List<TestRecord<String, String>> {
    return generateStringRecords(topic).take(num).toList()
}

fun generateStringRecords(topic: String): Sequence<TestRecord<String, String>> {
    return generateSequence(0L) {
        it + 1
    }.map {
        TestRecord(topic, "msg_$it", "msg_$it")
    }
}

fun <K, V> TestRecord<K, V>.toProducerRecord(): ProducerRecord<K, V> {
    return if (key == null) {
        ProducerRecord(topic, value)
    } else {
        ProducerRecord(topic, key, value)
    }
}

fun <K, V> ConsumerRecord<K, V>.toTestRecord(): TestRecord<K, V> {
    return TestRecord(topic(), key(), value())
}
