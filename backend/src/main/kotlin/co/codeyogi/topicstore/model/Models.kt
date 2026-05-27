package co.codeyogi.topicstore.model

import io.micronaut.serde.annotation.Serdeable

@Serdeable
data class TopicSpec(
    val name: String,
    val createdAt: String? = null,
    val active: Boolean = true,
)

@Serdeable
data class CreateTopicRequest(val name: String)

@Serdeable
data class EventRow(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: String?,
    val ts: String,
    val kafkaTs: String,
    val payload: String,
)

@Serdeable
data class LookupResponse(
    val total: Long,
    val rows: List<EventRow>,
)

@Serdeable
data class HealthResponse(
    val status: String,
    val clickhouse: Boolean,
    val kafka: Boolean,
    val activeTopics: List<String>,
)
