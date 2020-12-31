@file:JvmName("TestDataGenerator")

package dev.vasas.kafkasuite.tools

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord

data class TestRecord(val topic: String, val key: String?, val value: String)

fun stringRecordSequence(topic: String, count: Long? = null): Sequence<TestRecord> {
    val sequence = generateSequence(0L) {
        it + 1
    }.map {
        TestRecord(topic, "msg_$it", "msg_$it")
    }

    return if (count != null) sequence.take(count.toInt()) else sequence
}

fun TestRecord.toProducerRecord(): ProducerRecord<String, String> {
    return if (key == null) {
        ProducerRecord(topic, value)
    } else {
        ProducerRecord(topic, key, value)
    }
}

fun ConsumerRecord<String, String>.toTestRecord(): TestRecord {
    return TestRecord(topic(), key(), value())
}
