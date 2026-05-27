package co.codeyogi.topicstore.api

import co.codeyogi.topicstore.clickhouse.ClickHouseClient
import co.codeyogi.topicstore.model.LookupResponse
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.QueryValue

@Controller("/api/lookup")
class LookupController(private val clickhouse: ClickHouseClient) {

    @Get
    fun lookup(
        @QueryValue(defaultValue = "") topic: String,
        @QueryValue(defaultValue = "") key: String,
        @QueryValue(defaultValue = "") from: String,
        @QueryValue(defaultValue = "") to: String,
        @QueryValue(defaultValue = "") jsonPath: String,
        @QueryValue(defaultValue = "") jsonValue: String,
        @QueryValue(defaultValue = "100") limit: Int,
        @QueryValue(defaultValue = "0") offset: Int,
    ): LookupResponse {
        val safeLimit = limit.coerceIn(1, 1000)
        val safeOffset = offset.coerceAtLeast(0)
        val (total, rows) = clickhouse.lookup(
            ClickHouseClient.LookupFilter(
                topic = topic.ifBlank { null },
                key = key.ifBlank { null },
                from = from.ifBlank { null },
                to = to.ifBlank { null },
                jsonPath = jsonPath.ifBlank { null },
                jsonValue = jsonValue.ifBlank { null },
                limit = safeLimit,
                offset = safeOffset,
            )
        )
        return LookupResponse(total = total, rows = rows)
    }
}
