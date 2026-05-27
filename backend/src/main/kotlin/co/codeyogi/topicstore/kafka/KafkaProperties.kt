package co.codeyogi.topicstore.kafka

import io.micronaut.context.annotation.ConfigurationProperties

@ConfigurationProperties("kafka")
class KafkaProperties {
    var bootstrapServers: String = "kafka:9092"
    var groupId: String = "topicstore-consumer"
    var pollTimeoutMs: Long = 500
    var batchSize: Int = 500
    var batchFlushMs: Long = 1000
}

@ConfigurationProperties("ingest")
class IngestProperties {
    var refreshIntervalMs: Long = 5000
}
