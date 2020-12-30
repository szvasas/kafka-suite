package dev.vasas.kafkasuite.tools.producer

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import java.time.Duration
import java.util.Collections.synchronizedList
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong


class Metrics<K, V> {

    private val _sent: AtomicLong = AtomicLong(0)
    private val _delivered: AtomicLong = AtomicLong(0)
    private val _failed: AtomicLong = AtomicLong(0)
    private val _totalDurationInNanos: AtomicLong = AtomicLong(0)
    private val _deliveredRecords: MutableList<ProducerRecord<K, V>> = synchronizedList(mutableListOf())
    private val _exceptions: MutableList<Exception> = synchronizedList(mutableListOf())
    private val observers: MutableList<(Metrics<K, V>) -> Unit> = synchronizedList(mutableListOf())

    val sent: Long
        get() = _sent.get()
    val delivered: Long
        get() = _delivered.get()
    val failed: Long
        get() = _failed.get()
    val totalDuration: Duration
        get() = Duration.ofNanos(_totalDurationInNanos.get())
    val averageDuration: Duration
        get() = if (_sent.get() > 0) {
            totalDuration.dividedBy(_sent.get())
        } else {
            Duration.ZERO
        }
    val deliveredRecords: List<ProducerRecord<K, V>>
        get() = _deliveredRecords
    val exceptions: List<Exception>
        get() = _exceptions

    fun sent() {
        _sent.incrementAndGet()
        invokeObservers()
    }

    fun sendDuration(duration: Duration) {
        _totalDurationInNanos.addAndGet(duration.toNanos())
        invokeObservers()
    }

    fun delivered(record: ProducerRecord<K, V>) {
        _delivered.incrementAndGet()
        _deliveredRecords.add(record)
        invokeObservers()
    }

    fun failed(exception: Exception) {
        _failed.incrementAndGet()
        _exceptions.add(exception)
        invokeObservers()
    }

    fun addObserver(observer: (Metrics<K, V>) -> Unit) {
        observers.add(observer)
    }

    private fun invokeObservers() {
        observers.forEach { observer ->
            observer.invoke(this)
        }
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
        private val metrics: Metrics<K, V>
) : Producer<K, V> by delegate {

    override fun send(record: ProducerRecord<K, V>): Future<RecordMetadata> {
        return this.send(record, null)
    }

    override fun send(record: ProducerRecord<K, V>, callback: Callback?): Future<RecordMetadata> {
        val start = System.nanoTime()

        val result = delegate.send(record) { metadata, exception ->
            if (exception == null) {
                metrics.delivered(record)
            } else {
                metrics.failed(exception)
            }
            callback?.onCompletion(metadata, exception)
        }
        metrics.sent()
        result.get()
        metrics.sendDuration(Duration.ofNanos(System.nanoTime() - start))

        return result
    }

}

fun <K, V> Producer<K, V>.withMetricsDecorator(metrics: Metrics<K, V>): Producer<K, V> {
    return MetricsProducerDecorator(this, metrics)
}
