package dev.vasas.kafkasuite.demo.tools.producer

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import dev.vasas.kafkasuite.tools.generateStringRecords
import dev.vasas.kafkasuite.tools.createStringProducer
import dev.vasas.kafkasuite.tools.toProducerRecord
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import java.time.Duration.ofMillis
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

class ProducerDecoratorTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster()

    @Test
    fun `metrics and send rate decorators work as expected`() {
        val testTopic = UUID.randomUUID().toString()
        val generatedMessageCount = 20
        val sendRate = 100L
        val testMessages = generateStringRecords(testTopic, generatedMessageCount).map { it.toProducerRecord() }

        val metricsQueue = ConcurrentLinkedQueue<Metrics<String, String>>()
        kafkaCluster.createStringProducer()
                .withSendRateDecorator(sendRate)
                .withMetricsDecorator(metricsQueue)
                .use { producer ->
                    testMessages.forEach {
                        producer.send(it)
                    }
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
                    .describedAs("Delivered")
                    .isEqualTo(0)

            softly.assertThat(metrics.totalDuration)
                    .describedAs("Total duration")
                    .isGreaterThan(ofMillis(sendRate * generatedMessageCount))

            softly.assertThat(metrics.averageDuration)
                    .describedAs("Average duration")
                    .isGreaterThan(ofMillis(sendRate))

            softly.assertThat(metrics.deliveredRecords)
                    .describedAs("Delivered records")
                    .containsExactlyInAnyOrderElementsOf(testMessages.asIterable())

            softly.assertThat(metrics.exceptions)
                    .describedAs("Exceptions")
                    .isEmpty()

            softly.assertAll()
        }

    }
}