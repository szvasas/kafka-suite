package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import dev.vasas.kafkasuite.tools.createStringProducer
import dev.vasas.kafkasuite.tools.generateStringRecords
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.comparables.shouldBeGreaterThan
import io.kotest.matchers.longs.shouldBeExactly
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.time.Duration.ofMillis
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit.SECONDS
import java.util.concurrent.atomic.AtomicBoolean

class ProducerActorTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster()

    @Test
    fun `metrics collection is accurate`() {
        val testTopic = UUID.randomUUID().toString()
        val generatedMessageCount = 20
        val testMessages = generateStringRecords(testTopic, generatedMessageCount)

        val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()
        val producer = ProducerActor(
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
        assertSoftly {
            metrics.sent shouldBeExactly generatedMessageCount.toLong()
            metrics.delivered shouldBeExactly generatedMessageCount.toLong()
            metrics.failed shouldBeExactly 0
            metrics.totalDuration shouldBeGreaterThan ofMillis(generatedMessageCount.toLong())
            metrics.averageDuration shouldBeGreaterThan ofMillis(0L)
            metrics.deliveredRecords shouldContainAll testMessages.toList()
            metrics.exceptions.isEmpty() shouldBe true
        }
    }

    @Test
    @Timeout(1L, unit = SECONDS)
    fun `producer can be cancelled`() {
        val isActive = AtomicBoolean(true)
        val producer = ProducerActor(
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
        val producer = ProducerActor(
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

        duration shouldBeGreaterThan ofMillis(rate * numRecords)
    }
}
