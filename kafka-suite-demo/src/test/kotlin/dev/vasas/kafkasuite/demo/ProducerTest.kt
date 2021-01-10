package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import dev.vasas.kafkasuite.tools.createStringProducer
import dev.vasas.kafkasuite.tools.generateStringRecords
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.time.Duration.ofMillis
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean

class ProducerTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster()

    @Test
    fun `metrics collection is accurate`() {
        val testTopic = UUID.randomUUID().toString()
        val generatedMessageCount = 20
        val testMessages = generateStringRecords(testTopic, generatedMessageCount)

        val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()
        val producer = Producer(
                id = "firstProducer",
                kafkaProducer = kafkaCluster.createStringProducer(),
                records = testMessages,
                rate = 0L,
                metricsQueue = metricsQueue,
                isActive = AtomicBoolean(true)
        )
        runBlocking {
            producer.produce()
        }

        val metrics = metricsQueue.aggregate()
        assertSoftly { softly ->
            softly.assertThat(metrics.sent)
                    .describedAs("Sent")
                    .isEqualTo(generatedMessageCount.toLong())

            softly.assertThat(metrics.delivered)
                    .describedAs("Delivered")
                    .isEqualTo(generatedMessageCount.toLong())

            softly.assertThat(metrics.failed)
                    .describedAs("Failed")
                    .isEqualTo(0)

            softly.assertThat(metrics.totalDuration)
                    .describedAs("Total duration")
                    .isGreaterThan(ofMillis(generatedMessageCount.toLong()))

            softly.assertThat(metrics.averageDuration)
                    .describedAs("Average duration")
                    .isGreaterThan(ofMillis(0L))

            softly.assertThat(metrics.deliveredRecords)
                    .describedAs("Delivered records")
                    .containsExactlyInAnyOrderElementsOf(testMessages.asIterable())

            softly.assertThat(metrics.exceptions)
                    .describedAs("Exceptions")
                    .isEmpty()

            softly.assertAll()
        }
    }

    @Test
    @Timeout(1L, unit = SECONDS)
    fun `producer can be cancelled`() {
        val isActive = AtomicBoolean(true)
        val producer = Producer(
                id = "firstProducer",
                kafkaProducer = kafkaCluster.createStringProducer(),
                records = generateStringRecords(UUID.randomUUID().toString()),
                rate = 0L,
                metricsQueue = ConcurrentLinkedQueue(),
                isActive = isActive
        )

        GlobalScope.launch {
            delay(500L)
            isActive.set(false)
        }
        runBlocking {
            producer.produce()
        }
    }

    @Test
    fun `produce rate can be specified`() {
        val rate = 100L
        val numRecords = 20
        val producer = Producer(
                id = "firstProducer",
                kafkaProducer = kafkaCluster.createStringProducer(),
                records = generateStringRecords(UUID.randomUUID().toString(), numRecords),
                rate = rate,
                metricsQueue = ConcurrentLinkedQueue(),
                isActive = AtomicBoolean(true)
        )

        val start = System.nanoTime()
        runBlocking {
            producer.produce()
        }
        val duration = Duration.ofNanos(System.nanoTime() - start)

        assertThat(duration).isGreaterThan(ofMillis(rate * numRecords))
    }
}
