package co.codeyogi.topicstore.registry

import co.codeyogi.topicstore.clickhouse.ClickHouseClient
import co.codeyogi.topicstore.kafka.ConsumerManager
import io.micronaut.context.event.ApplicationEventListener
import io.micronaut.context.event.StartupEvent
import io.micronaut.scheduling.annotation.Scheduled
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory

@Singleton
class TopicRegistry(
    private val clickhouse: ClickHouseClient,
    private val consumerManager: ConsumerManager,
) : ApplicationEventListener<StartupEvent> {

    private val log = LoggerFactory.getLogger(javaClass)

    override fun onApplicationEvent(event: StartupEvent) {
        sync()
    }

    @Scheduled(fixedDelay = "5s", initialDelay = "10s")
    fun sync() {
        try {
            val topics = clickhouse.listTopics(activeOnly = true).map { it.name }.toSet()
            consumerManager.setDesiredTopics(topics)
        } catch (e: Exception) {
            log.warn("Topic registry sync failed: ${e.message}")
        }
    }

    fun list(): List<co.codeyogi.topicstore.model.TopicSpec> =
        clickhouse.listTopics(activeOnly = false)

    fun add(name: String) {
        validate(name)
        clickhouse.upsertTopic(name, active = true)
        sync()
    }

    fun remove(name: String) {
        clickhouse.upsertTopic(name, active = false)
        sync()
    }

    private fun validate(name: String) {
        require(name.isNotBlank()) { "topic name must not be blank" }
        require(name.length <= 249) { "topic name too long" }
        require(name.all { it.isLetterOrDigit() || it == '.' || it == '_' || it == '-' }) {
            "topic name may only contain letters, digits, '.', '_', '-'"
        }
    }
}
