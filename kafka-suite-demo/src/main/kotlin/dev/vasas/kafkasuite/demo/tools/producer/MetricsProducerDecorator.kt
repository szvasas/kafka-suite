package dev.vasas.kafkasuite.demo.tools.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.Future

data class Metrics<K, V>(
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

    private val logger = LoggerFactory.getLogger(MetricsProducerDecorator::class.java)

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
        try {
            result.get()
        } catch (e: Exception) {
            logger.error(e.message, e)
        }
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

fun <K, V> MutableCollection<Metrics<K, V>>.aggregate(): Metrics<K, V> {
    var result = Metrics<K, V>()

    val iterator = this.iterator()
    while (iterator.hasNext()) {
        result += iterator.next()
        iterator.remove()
    }

    return result
}
