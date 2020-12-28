package dev.vasas.kafkasuite.extension.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.concurrent.Future


data class Metrics<K, V> @JvmOverloads constructor(
        var sent: Long = 0,
        var delivered: Long = 0,
        var failed: Long = 0,
        var totalDuration: Duration = Duration.ZERO,
        val deliveredRecords: MutableList<ProducerRecord<K, V>> = mutableListOf(),
        val exceptions: MutableList<Exception> = mutableListOf()
) {
    val averageDuration: Duration
        get() = if (sent > 0) {
            totalDuration.dividedBy(sent)
        } else {
            Duration.ZERO
        }
}

class MetricsProducerDecorator<K, V>(
        private val delegate: Producer<K, V>,
        private val metrics: Metrics<K, V>
) : Producer<K, V> by delegate {

    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> {
        return this.send(record, null)
    }

    override fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata> {
        val start = System.nanoTime()

        val result = delegate.send(record) { metadata, exception ->
            if (exception == null) {
                metrics.deliveredRecords.add(record)
                metrics.delivered++
            } else {
                metrics.exceptions.add(exception)
                metrics.failed++
            }
            callback?.onCompletion(metadata, exception)
        }
        metrics.sent++
        result.get()
        metrics.totalDuration = metrics.totalDuration.plusNanos(System.nanoTime() - start)

        return result
    }

}

fun <K, V> Producer<K, V>.withMetricsDecorator(metrics: Metrics<K, V>): Producer<K, V> {
    return MetricsProducerDecorator(this, metrics)
}
