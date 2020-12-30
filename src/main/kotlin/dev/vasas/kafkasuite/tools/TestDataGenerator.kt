@file:JvmName("TestDataGenerator")

package dev.vasas.kafkasuite.tools

import org.apache.kafka.clients.producer.ProducerRecord

fun stringMessageSequence(topic: String, count: Long? = null): Sequence<ProducerRecord<String, String>> {
    val sequence = generateSequence(0L) {
        it + 1
    }.map {
        ProducerRecord(topic, "msg_$it", "msg_$it")
    }

    return if (count != null) sequence.take(count.toInt()) else sequence
}
