package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.tools.TestRecord
import java.time.Duration

data class Metrics<K, V>(
        val sent: Long = 0,
        val delivered: Long = 0,
        val failed: Long = 0,
        val totalDuration: Duration = Duration.ZERO,
        val deliveredRecords: List<TestRecord<K, V>> = listOf(),
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

fun <K, V> MutableCollection<Metrics<K, V>>.aggregate(): Metrics<K, V> {
    var result = Metrics<K, V>()

    val iterator = this.iterator()
    while (iterator.hasNext()) {
        result += iterator.next()
        iterator.remove()
    }

    return result
}
