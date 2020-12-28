package dev.vasas.kafkasuite.extension.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.Collections.synchronizedList
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong


data class Metrics<K, V> @JvmOverloads constructor(
        var sent: AtomicLong = AtomicLong(0),
        var delivered: AtomicLong = AtomicLong(0),
        var failed: AtomicLong = AtomicLong(0),
        var totalDurationInNanos: AtomicLong = AtomicLong(0),
        val deliveredRecords: MutableList<ProducerRecord<K, V>> = synchronizedList(mutableListOf()),
        val exceptions: MutableList<Exception> = synchronizedList(mutableListOf())
) {
    val totalDuration: Duration
        get() = Duration.ofNanos(totalDurationInNanos.get())

    val averageDuration: Duration
        get() = if (sent.get() > 0) {
            totalDuration.dividedBy(sent.get())
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
                metrics.delivered.incrementAndGet()
            } else {
                metrics.exceptions.add(exception)
                metrics.failed.incrementAndGet()
            }
            callback?.onCompletion(metadata, exception)
        }
        metrics.sent.incrementAndGet()
        result.get()
        metrics.totalDurationInNanos.addAndGet(System.nanoTime() - start)

        return result
    }

}

fun <K, V> Producer<K, V>.withMetricsDecorator(metrics: Metrics<K, V>): Producer<K, V> {
    return MetricsProducerDecorator(this, metrics)
}
