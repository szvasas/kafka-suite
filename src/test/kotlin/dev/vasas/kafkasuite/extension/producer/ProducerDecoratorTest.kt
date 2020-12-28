package dev.vasas.kafkasuite.extension.producer

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import org.apache.kafka.clients.producer.ProducerRecord
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import java.time.Duration.ofMillis
import java.util.*

class ProducerDecoratorTest : KafkaSuite {

    override val kafkaCluster = createDockerKafkaCluster()

    @Test
    fun `metrics and send rate decorators work as expected`() {
        val testTopic = UUID.randomUUID().toString()
        val generatedMessageCount = 20L
        val sendRate = 100L
        val testMessages = createMessageSequence(testTopic, generatedMessageCount)

        val metrics = Metrics<String, String>()
        kafkaCluster.createStringProducer()
                .withSendRateDecorator(sendRate)
                .withMetricsDecorator(metrics)
                .use { producer ->
                    testMessages.forEach {
                        producer.send(it)
                    }
                }

        assertSoftly { softly ->
            softly.assertThat(metrics.sent)
                    .describedAs("Sent")
                    .isEqualTo(generatedMessageCount)

            softly.assertThat(metrics.delivered)
                    .describedAs("Delivered")
                    .isEqualTo(generatedMessageCount)

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

    private fun createMessageSequence(testTopic: String, count: Long): Sequence<ProducerRecord<String, String>> {
        val sequence = generateSequence(0L) {
            it + 1
        }.map {
            ProducerRecord(testTopic, "msg_$it", "msg_$it")
        }
        return sequence.take(count.toInt())
    }

}
