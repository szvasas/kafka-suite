package dev.vasas.kafkasuite.demo

import dev.vasas.kafkasuite.cluster.DockerKafkaCluster
import dev.vasas.kafkasuite.tools.TestRecord
import dev.vasas.kafkasuite.tools.createStringProducer
import dev.vasas.kafkasuite.tools.createTopic
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean

data class Topic(
        val numPartitions: Int = 1,
        val replicationFactor: Short = 1,
        val minInSyncReplicas: Short = 1,
        val name: String = "topic"
)

data class Producer(
        val recordGenerator: (List<Topic>) -> Sequence<TestRecord<String, String>>,
        val rate: Long,
        val config: Map<String, Any>,
        val id: String = "producer",
)

data class ActionContext(
        val kafkaCluster: DockerKafkaCluster,
        val metricsMap: Map<String, Metrics<String, String>>,
        val producerSwitches: Map<String, AtomicBoolean>,
        val metrics: Metrics<String, String> = metricsMap.entries.first().value,
        val producerSwitch: AtomicBoolean = producerSwitches.entries.first().value
)

data class Action(
        val trigger: (ActionContext) -> Boolean,
        val action: suspend (ActionContext) -> Unit,
        var executed: Boolean = false
)

data class Scenario(
        val kafkaCluster: DockerKafkaCluster,
        val topics: List<Topic>,
        val producers: List<Producer>,
        val actions: List<Action>
) {

    private val producerSwitches: MutableMap<String, AtomicBoolean> = mutableMapOf()
    private val producerMetricsQueues: MutableMap<String, Queue<Metrics<String, String>>> = mutableMapOf()
    private val runningMetrics: MutableMap<String, Metrics<String, String>> = mutableMapOf()

    fun run() {
        kafkaCluster.start()
        topics.forEach {
            kafkaCluster.createTopic(it.name, it.numPartitions, it.replicationFactor, it.minInSyncReplicas)
        }
        startProducerActors()
        runActionLoop()
    }

    private fun runActionLoop() = runBlocking {
        while (producerSwitches.atLeastOneIsOn()) {
            actions.filterNot(Action::executed)
                    .forEach { action ->
                        aggregateMetrics()

                        val actionContext = ActionContext(kafkaCluster, runningMetrics, producerSwitches)
                        if (action.trigger(actionContext)) {
                            GlobalScope.launch {
                                action.action(actionContext)
                            }
                            action.executed = true
                        }
                    }

            delay(100L)
        }

        println(runningMetrics.values.first())
    }

    private fun aggregateMetrics() {
        producerMetricsQueues.forEach { entry ->
            runningMetrics[entry.key] = runningMetrics.getValue(entry.key) + entry.value.aggregate()
        }
    }

    private fun startProducerActors() {
        val producerActors = producers.map {
            ProducerActor(
                    id = it.id,
                    kafkaProducer = kafkaCluster.createStringProducer(it.config),
                    records = it.recordGenerator(topics),
                    rate = it.rate,
                    metricsQueue = ConcurrentLinkedQueue(),
                    isActive = AtomicBoolean(true)
            )
        }

        producerActors.forEach { producerActor ->
            with(producerActor) {
                producerSwitches[id] = isActive
                producerMetricsQueues[id] = metricsQueue
                runningMetrics[id] = Metrics()

                GlobalScope.launch {
                    produce()
                }
            }
        }
    }

    private fun MutableMap<String, AtomicBoolean>.atLeastOneIsOn(): Boolean {
        return this.values.any(AtomicBoolean::get)
    }

}
