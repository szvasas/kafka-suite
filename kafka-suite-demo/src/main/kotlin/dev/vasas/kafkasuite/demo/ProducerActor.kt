package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.tools.TestRecord
import dev.vasas.kafkasuite.tools.toProducerRecord
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicBoolean

class ProducerActor<K, V>(
        val id: String,
        val kafkaProducer: org.apache.kafka.clients.producer.Producer<K, V>,
        val records: Sequence<TestRecord<K, V>>,
        val rate: Long,
        val metricsQueue: Queue<Metrics<K, V>>,
        val isActive: AtomicBoolean,
) {

    private val logger = LoggerFactory.getLogger(ProducerActor::class.java)

    suspend fun produce() {
        records.forEach {
            if (!isActive.get()) {
                return
            }
            produceSingle(it)
            delay(rate)
        }
    }

    private suspend fun produceSingle(record: TestRecord<K, V>) {
        val promise = CompletableFuture<RecordMetadata>();
        val deliveredRecords: MutableList<TestRecord<K, V>> = mutableListOf()
        val raisedExceptions: MutableList<Exception> = mutableListOf()

        val start = System.nanoTime()
        kafkaProducer.send(record.toProducerRecord()) { metadata, exception ->
            if (exception == null) {
                deliveredRecords.add(record)
                promise.complete(metadata)
            } else {
                raisedExceptions.add(exception)
                promise.completeExceptionally(exception)
            }
        }
        try {
            promise.await()
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
    }
}
