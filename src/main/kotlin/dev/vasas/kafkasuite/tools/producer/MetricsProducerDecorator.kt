package dev.vasas.kafkasuite.tools.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.*
import java.util.concurrent.Future

data class Metrics<K, V> @JvmOverloads constructor(
        val sent: Long = 0,
        val delivered: Long = 0,
        val failed: Long = 0,
        val totalDuration: Duration = Duration.ZERO,
        val deliveredRecords: List<ProducerRecord<K, V>> = listOf(),
        val exceptions: List<Exception> = listOf()
) {
    val averageDuration: Duration
        get() = if (sent > 0) {
            totalDuration.dividedBy(sent)
        } else {
            Duration.ZERO
        }

    fun aggregate(metrics: MutableCollection<Metrics<K, V>>): Metrics<K, V> {
        var result = this

        val iterator = metrics.iterator()
        while (iterator.hasNext()) {
            result += iterator.next()
            iterator.remove()
        }

        return result
    }

    operator fun plus(other: Metrics<K, V>): Metrics<K, V> {
        return Metrics(
                sent = this.sent + other.sent,
                delivered = this.delivered + other.delivered,
                failed = this.failed + other.failed,
                totalDuration = this.totalDuration + other.totalDuration,
                deliveredRecords = this.deliveredRecords + other.deliveredRecords,
                exceptions = this.exceptions + other.exceptions
        )
    }

    override fun toString(): String {
        return """
            Sent: $sent
            Delivered: $delivered
            Failed: $failed
            Total duration: ${totalDuration.toMillis()} milliseconds
            Average duration: ${averageDuration.toMillis()} milliseconds
        """.trimIndent()
    }
}

class MetricsProducerDecorator<K, V>(
        private val delegate: Producer<K, V>,
        private val metricsQueue: Queue<Metrics<K, V>>
) : Producer<K, V> by delegate {

    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> {
        return this.send(record, null)
    }

    override fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata> {
        val start = System.nanoTime()

        val deliveredRecords: MutableList<ProducerRecord<K, V>> = mutableListOf()
        val raisedExceptions: MutableList<Exception> = mutableListOf()

        val result = delegate.send(record) { metadata, exception ->
            if (exception == null) {
                deliveredRecords.add(record)
            } else {
                raisedExceptions.add(exception)
            }
            callback?.onCompletion(metadata, exception)
        }
        result.get()
        val duration = Duration.ofNanos(System.nanoTime() - start)

        metricsQueue.offer(Metrics(
                sent = 1,
                delivered = deliveredRecords.size.toLong(),
                failed = raisedExceptions.size.toLong(),
                totalDuration = duration,
                deliveredRecords = deliveredRecords,
                exceptions = raisedExceptions
        ))

        return result
    }

}

fun <K, V> Producer<K, V>.withMetricsDecorator(metricsQueue: Queue<Metrics<K, V>>): Producer<K, V> {
    return MetricsProducerDecorator(this, metricsQueue)
}
