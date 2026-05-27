package co.codeyogi.topicstore.ui

import kotlinx.coroutines.await
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

private val json = Json { ignoreUnknownKeys = true; isLenient = true }

@Serializable
data class TopicSpec(val name: String, val createdAt: String? = null, val active: Boolean = true)

@Serializable
data class CreateTopicRequest(val name: String)

@Serializable
data class EventRow(
    val topic: String,
    val partition: Int,
    val offset: Long,
    val key: String? = null,
    val ts: String,
    val kafkaTs: String,
    val payload: String,
)

@Serializable
data class LookupResponse(val total: Long, val rows: List<EventRow>)

@Serializable
data class HealthResponse(
    val status: String,
    val clickhouse: Boolean,
    val kafka: Boolean,
    val activeTopics: List<String>,
)

object Api {
    private suspend fun getText(url: String): String {
        val resp = js("fetch")(url).unsafeCast<kotlin.js.Promise<dynamic>>().await()
        if (resp.ok != true) error("HTTP ${resp.status}")
        return resp.text().unsafeCast<kotlin.js.Promise<String>>().await()
    }

    private suspend fun sendJson(url: String, method: String, body: String?): String {
        val init: dynamic = js("({})")
        init.method = method
        val headers: dynamic = js("({})")
        headers["Content-Type"] = "application/json"
        init.headers = headers
        if (body != null) init.body = body
        val resp = js("fetch")(url, init).unsafeCast<kotlin.js.Promise<dynamic>>().await()
        if (resp.ok != true) error("HTTP ${resp.status}")
        return resp.text().unsafeCast<kotlin.js.Promise<String>>().await()
    }

    suspend fun listTopics(): List<TopicSpec> =
        json.decodeFromString(getText("/api/topics"))

    suspend fun addTopic(name: String) {
        val body = json.encodeToString(CreateTopicRequest.serializer(), CreateTopicRequest(name))
        sendJson("/api/topics", "POST", body)
    }

    suspend fun deleteTopic(name: String) {
        sendJson("/api/topics/${encodeURIComponent(name)}", "DELETE", null)
    }

    suspend fun lookup(
        topic: String?, key: String?, from: String?, to: String?,
        jsonPath: String?, jsonValue: String?, limit: Int, offset: Int,
    ): LookupResponse {
        val qs = buildString {
            append("?limit=").append(limit).append("&offset=").append(offset)
            if (!topic.isNullOrBlank()) append("&topic=").append(encodeURIComponent(topic))
            if (!key.isNullOrBlank()) append("&key=").append(encodeURIComponent(key))
            if (!from.isNullOrBlank()) append("&from=").append(encodeURIComponent(from))
            if (!to.isNullOrBlank()) append("&to=").append(encodeURIComponent(to))
            if (!jsonPath.isNullOrBlank()) append("&jsonPath=").append(encodeURIComponent(jsonPath))
            if (!jsonValue.isNullOrBlank()) append("&jsonValue=").append(encodeURIComponent(jsonValue))
        }
        return json.decodeFromString(getText("/api/lookup$qs"))
    }

    suspend fun health(): HealthResponse =
        json.decodeFromString(getText("/api/health"))
}

private fun encodeURIComponent(s: String): String =
    js("encodeURIComponent")(s).unsafeCast<String>()
