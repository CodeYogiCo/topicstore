package co.codeyogi.topicstore.clickhouse

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("clickhouse")
class ClickHouseProperties {
    var url: String = "jdbc:ch://clickhouse:8123/default"
    var user: String = "default"
    var password: String = ""
    var database: String = "default"
}
