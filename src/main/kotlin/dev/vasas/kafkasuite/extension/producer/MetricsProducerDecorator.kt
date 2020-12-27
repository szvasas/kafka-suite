package dev.vasas.kafkasuite.extension.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.concurrent.Future


data class Metrics(
        var eventCount: Long = 0,
        var totalDuration: Duration = Duration.ZERO,
        val exceptions: MutableList<Exception> = mutableListOf()
) {
    val averageDuration: Duration
        get() = if (eventCount > 0){
            totalDuration.dividedBy(eventCount)
        } else {
            Duration.ZERO
        }
}

class MetricsProducerDecorator<K, V>(
        private val delegate: Producer<K, V>,
        private val metrics: Metrics
) : Producer<K, V> by delegate {

    override fun send(record: ProducerRecord<K, V>?): Future<RecordMetadata> {
        return this.send(record, null)
    }

    override fun send(record: ProducerRecord<K, V>?, callback: Callback?): Future<RecordMetadata> {
        val start = System.nanoTime()

        val result = delegate.send(record) { metadata, exception ->
            exception?.let { metrics.exceptions.add(exception) }
            callback?.onCompletion(metadata, exception)
        }
        result.get()
        metrics.totalDuration = metrics.totalDuration.plusNanos(System.nanoTime() - start)
        metrics.eventCount++

        return result
    }

}

fun <K, V> Producer<K, V>.withMetricsDecorator(metrics: Metrics): Producer<K, V> {
    return MetricsProducerDecorator(this, metrics)
}
