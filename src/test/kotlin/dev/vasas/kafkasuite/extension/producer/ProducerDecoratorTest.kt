package dev.vasas.kafkasuite.extension.producer

import dev.vasas.kafkasuite.cluster.createDockerKafkaCluster
import dev.vasas.kafkasuite.extension.createStringProducer
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
        val generatedMessageCount = 25L
        val sendRate = 200L
        val testMessages = createMessageSequence(testTopic, generatedMessageCount)
        val deltaCoEff = 1.2

        val metrics = Metrics()
        kafkaCluster.createStringProducer()
                .withSendRateDecorator(sendRate)
                .withMetricsDecorator(metrics)
                .use { producer ->
                    testMessages.forEach {
                        producer.send(it)
                    }
                }

        assertSoftly { softly ->
            softly.assertThat(metrics.eventCount).isEqualTo(generatedMessageCount)
            softly.assertThat(metrics.totalDuration).isBetween(
                    ofMillis(sendRate * generatedMessageCount),
                    ofMillis((sendRate * generatedMessageCount * deltaCoEff).toLong())
            )
            softly.assertThat(metrics.averageDuration).isBetween(
                    ofMillis(sendRate),
                    ofMillis((sendRate * deltaCoEff).toLong())
            )
            softly.assertThat(metrics.exceptions).isEmpty()

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
