package co.codeyogi.topicstore.kafka

import co.codeyogi.topicstore.clickhouse.ClickHouseClient
import co.codeyogi.topicstore.model.EventRow
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.concurrent.thread

@Singleton
class ConsumerManager(
    private val kafkaProps: KafkaProperties,
    private val ingestProps: IngestProperties,
    private val clickhouse: ClickHouseClient,
) : ApplicationEventListener<StartupEvent> {

    private val log = LoggerFactory.getLogger(javaClass)
    private val running = AtomicBoolean(false)
    private val desiredTopics = AtomicReference<Set<String>>(emptySet())
    private val currentTopics = AtomicReference<Set<String>>(emptySet())
    private var consumer: KafkaConsumer<String, String>? = null
    private var worker: Thread? = null

    override fun onApplicationEvent(event: StartupEvent) {
        start()
    }

    fun start() {
        if (!running.compareAndSet(false, true)) return
        log.info("Starting Kafka consumer manager (bootstrap=${kafkaProps.bootstrapServers})")
        consumer = buildConsumer()
        worker = thread(name = "kafka-ingest", isDaemon = true) { runLoop() }
    }

    @PreDestroy
    fun stop() {
        if (!running.compareAndSet(true, false)) return
        log.info("Stopping Kafka consumer manager")
        consumer?.wakeup()
        worker?.join(5000)
    }

    fun setDesiredTopics(topics: Set<String>) {
        desiredTopics.set(topics)
        log.info("Desired topics updated: $topics")
    }

    fun activeTopics(): Set<String> = currentTopics.get()

    private fun buildConsumer(): KafkaConsumer<String, String> {
        val p = Properties().apply {
            put("bootstrap.servers", kafkaProps.bootstrapServers)
            put("group.id", kafkaProps.groupId)
            put("enable.auto.commit", "false")
            put("auto.offset.reset", "earliest")
            put("max.poll.records", kafkaProps.batchSize.toString())
            put("key.deserializer", StringDeserializer::class.java.name)
            put("value.deserializer", StringDeserializer::class.java.name)
        }
        return KafkaConsumer(p)
    }

    private fun runLoop() {
        val c = consumer ?: return
        val pending = mutableListOf<EventRow>()
        var lastFlush = System.currentTimeMillis()
        var lastTopicRefresh = 0L

        try {
            while (running.get()) {
                val now = System.currentTimeMillis()

                if (now - lastTopicRefresh >= ingestProps.refreshIntervalMs) {
                    val desired = desiredTopics.get()
                    val current = currentTopics.get()
                    if (desired != current) {
                        log.info("Re-subscribing: $current -> $desired")
                        if (desired.isEmpty()) c.unsubscribe() else c.subscribe(desired)
                        currentTopics.set(desired)
                    }
                    lastTopicRefresh = now
                }

                if (currentTopics.get().isEmpty()) {
                    Thread.sleep(200)
                    continue
                }

                val records = try {
                    c.poll(Duration.ofMillis(kafkaProps.pollTimeoutMs))
                } catch (e: WakeupException) {
                    if (!running.get()) break else throw e
                }

                for (r in records) {
                    pending += EventRow(
                        topic = r.topic(),
                        partition = r.partition(),
                        offset = r.offset(),
                        key = r.key(),
                        ts = Instant.now().toString(),
                        kafkaTs = Instant.ofEpochMilli(r.timestamp()).toString(),
                        payload = r.value() ?: "",
                    )
                }

                val timeUp = (System.currentTimeMillis() - lastFlush) >= kafkaProps.batchFlushMs
                if (pending.size >= kafkaProps.batchSize || (pending.isNotEmpty() && timeUp)) {
                    try {
                        clickhouse.insertEvents(pending)
                        c.commitSync()
                        log.debug("Flushed ${pending.size} events to ClickHouse")
                        pending.clear()
                        lastFlush = System.currentTimeMillis()
                    } catch (e: Exception) {
                        log.error("Flush failed; will retry next iteration", e)
                        Thread.sleep(1000)
                    }
                }
            }
        } catch (e: WakeupException) {
            log.info("Consumer woken for shutdown")
        } catch (e: Exception) {
            log.error("Consumer loop crashed", e)
        } finally {
            try {
                if (pending.isNotEmpty()) clickhouse.insertEvents(pending)
                c.commitSync()
            } catch (e: Exception) {
                log.warn("Final flush failed", e)
            }
            c.close(Duration.ofSeconds(5))
            log.info("Kafka consumer closed")
        }
    }

    fun kafkaReachable(): Boolean = try {
        val props = Properties().apply {
            put("bootstrap.servers", kafkaProps.bootstrapServers)
            put("request.timeout.ms", "2000")
        }
        org.apache.kafka.clients.admin.AdminClient.create(props).use { admin ->
            admin.listTopics().names().get(3, java.util.concurrent.TimeUnit.SECONDS)
            true
        }
    } catch (e: Exception) {
        false
    }
}
