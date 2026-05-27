package co.codeyogi.topicstore.api

import co.codeyogi.topicstore.clickhouse.ClickHouseClient
import co.codeyogi.topicstore.kafka.ConsumerManager
import co.codeyogi.topicstore.model.HealthResponse
import io.micronaut.http.HttpResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get

@Controller("/api/health")
class HealthController(
    private val clickhouse: ClickHouseClient,
    private val consumerManager: ConsumerManager,
) {
    @Get
    fun health(): HttpResponse<HealthResponse> {
        val ch = clickhouse.ping()
        val kf = consumerManager.kafkaReachable()
        val status = if (ch && kf) "UP" else "DEGRADED"
        val resp = HealthResponse(
            status = status,
            clickhouse = ch,
            kafka = kf,
            activeTopics = consumerManager.activeTopics().sorted(),
        )
        return if (ch && kf) HttpResponse.ok(resp) else HttpResponse.serverError(resp)
    }
}
