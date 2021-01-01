package dev.vasas.kafkasuite.demo.tools.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.Future

class SendRateProducerDecorator<K, V>(
        private val delegate: Producer<K, V>,
        private val rate: Long
) : Producer<K, V> by delegate {

    override fun send(record: ProducerRecord<K, V>?): Future<RecordMetadata> {
        return this.send(record, null)
    }

    override fun send(record: ProducerRecord<K, V>?, callback: Callback?): Future<RecordMetadata> {
        val result = delegate.send(record, callback)
        Thread.sleep(rate)
        return result
    }

}

fun <K, V> Producer<K, V>.withSendRateDecorator(rate: Long): Producer<K, V> {
    return SendRateProducerDecorator(this, rate)
}
