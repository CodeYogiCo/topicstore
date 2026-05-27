package co.codeyogi.topicstore

import co.codeyogi.topicstore.model.CreateTopicRequest
import co.codeyogi.topicstore.model.LookupResponse
import co.codeyogi.topicstore.model.TopicSpec
import io.micronaut.context.ApplicationContext
import io.micronaut.http.HttpRequest
import io.micronaut.http.client.HttpClient
import io.micronaut.runtime.server.EmbeddedServer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
class EndToEndTest {

    private lateinit var kafka: KafkaContainer
    private lateinit var clickhouse: GenericContainer<*>
    private lateinit var server: EmbeddedServer
    private lateinit var client: HttpClient

    @BeforeAll
    fun setup() {
        kafka = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
        kafka.start()
        clickhouse = GenericContainer(DockerImageName.parse("clickhouse/clickhouse-server:24.3"))
            .withExposedPorts(8123, 9000)
            .withEnv("CLICKHOUSE_DB", "default")
            .withEnv("CLICKHOUSE_USER", "ts")
            .withEnv("CLICKHOUSE_PASSWORD", "ts")
            .waitingFor(Wait.forHttp("/ping").forPort(8123).forStatusCode(200))
        clickhouse.start()

        val chJdbc = "jdbc:ch://${clickhouse.host}:${clickhouse.getMappedPort(8123)}/default"
        server = ApplicationContext.run(
            EmbeddedServer::class.java,
            mapOf(
                "kafka.bootstrap-servers" to kafka.bootstrapServers,
                "clickhouse.url" to chJdbc,
                "clickhouse.user" to "ts",
                "clickhouse.password" to "ts",
                "ingest.refresh-interval-ms" to 500,
                "kafka.batch-flush-ms" to 250,
                "kafka.batch-size" to 50,
                "micronaut.server.port" to -1,
            ),
        )
        client = server.applicationContext.createBean(HttpClient::class.java, server.uri)
    }

    @AfterAll
    fun teardown() {
        if (::client.isInitialized) client.close()
        if (::server.isInitialized) server.stop()
        if (::clickhouse.isInitialized) clickhouse.stop()
        if (::kafka.isInitialized) kafka.stop()
    }

    @Test
    fun `produces to kafka, registers topic, finds rows in clickhouse via lookup`() {
        val topic = "e2e.events"

        produce(topic, "before-add", """{"phase":"before-add"}""")

        val created = client.toBlocking().exchange(
            HttpRequest.POST("/api/topics", CreateTopicRequest(topic)),
            TopicSpec::class.java,
        )
        assertThat(created.status.code).isEqualTo(201)

        await().atMost(Duration.ofSeconds(15)).until { listActive().contains(topic) }

        repeat(10) { i ->
            produce(topic, "key-$i", """{"i":$i,"user":{"id":"u${i % 3}"},"v":"hello"}""")
        }

        await().atMost(Duration.ofSeconds(20)).until {
            lookup(topic = topic).total >= 10L
        }

        val u1 = lookup(topic = topic, jsonPath = "user.id", jsonValue = "u1")
        assertThat(u1.total).isGreaterThan(0L)
        assertThat(u1.rows).allMatch { it.payload.contains("\"id\":\"u1\"") }

        client.toBlocking().exchange<Any, Any>(HttpRequest.DELETE<Any>("/api/topics/$topic"))
        await().atMost(Duration.ofSeconds(10)).until { !listActive().contains(topic) }
    }

    private fun produce(topic: String, key: String, value: String) {
        val props = Properties().apply {
            put("bootstrap.servers", kafka.bootstrapServers)
            put("key.serializer", StringSerializer::class.java.name)
            put("value.serializer", StringSerializer::class.java.name)
            put("acks", "all")
        }
        KafkaProducer<String, String>(props).use { p ->
            p.send(ProducerRecord(topic, key, value)).get()
            p.flush()
        }
    }

    private fun listActive(): List<String> {
        val r = client.toBlocking().retrieve(
            HttpRequest.GET<Any>("/api/topics"),
            Array<TopicSpec>::class.java,
        )
        return r.filter { it.active }.map { it.name }
    }

    private fun lookup(
        topic: String? = null,
        key: String? = null,
        jsonPath: String? = null,
        jsonValue: String? = null,
    ): LookupResponse {
        val params = buildList {
            topic?.let { add("topic=$it") }
            key?.let { add("key=$it") }
            jsonPath?.let { add("jsonPath=$it") }
            jsonValue?.let { add("jsonValue=$it") }
        }
        val qs = if (params.isEmpty()) "" else "?" + params.joinToString("&")
        return client.toBlocking().retrieve(
            HttpRequest.GET<Any>("/api/lookup$qs"),
            LookupResponse::class.java,
        )
    }
}
