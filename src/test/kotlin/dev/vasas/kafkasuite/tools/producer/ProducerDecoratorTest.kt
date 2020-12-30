package dev.vasas.kafkasuite.tools.producer

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.junit5.KafkaSuite
import dev.vasas.kafkasuite.tools.stringMessageSequence
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
        val testMessages = stringMessageSequence(testTopic, generatedMessageCount)

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
            softly.assertThat(metrics.sent.get())
                    .describedAs("Sent")
                    .isEqualTo(generatedMessageCount)

            softly.assertThat(metrics.delivered.get())
                    .describedAs("Delivered")
                    .isEqualTo(generatedMessageCount)

            softly.assertThat(metrics.failed.get())
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
