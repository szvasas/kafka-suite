@file:JvmName("TestDataGenerator")

package dev.vasas.kafkasuite.tools

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

data class TestRecord<K, V>(val topic: String, val key: K?, val value: V)

@JvmOverloads
fun generateStringRecords(topic: String, num: Int? = null, prefix: String = "msg"): Sequence<TestRecord<String, String>> {
    val sequence = generateSequence(0L) {
        it + 1
    }.map {
        TestRecord(topic, "$prefix-$it", "$prefix-$it")
    }

    return if (num == null) sequence else sequence.take(num)
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
