package co.codeyogi.topicstore.clickhouse

import co.codeyogi.topicstore.model.EventRow
import jakarta.annotation.PostConstruct
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.Instant

@Singleton
class ClickHouseClient(private val props: ClickHouseProperties) {
    private val log = LoggerFactory.getLogger(javaClass)

    fun connect(): Connection {
        val p = java.util.Properties().apply {
            setProperty("user", props.user)
            setProperty("password", props.password)
        }
        return DriverManager.getConnection(props.url, p)
    }

    @PostConstruct
    fun init() {
        var attempt = 0
        val maxAttempts = 30
        while (true) {
            try {
                connect().use { c ->
                    c.createStatement().use { st ->
                        st.execute(
                            """
                            CREATE TABLE IF NOT EXISTS kafka_events (
                                topic String,
                                partition Int32,
                                offset Int64,
                                `key` Nullable(String),
                                ts DateTime64(3) DEFAULT now64(3),
                                kafka_ts DateTime64(3),
                                payload String
                            ) ENGINE = MergeTree()
                            PARTITION BY toYYYYMMDD(ts)
                            ORDER BY (topic, ts, partition, offset)
                            """.trimIndent()
                        )
                        st.execute(
                            """
                            CREATE TABLE IF NOT EXISTS topic_registry (
                                name String,
                                created_at DateTime64(3) DEFAULT now64(3),
                                active UInt8 DEFAULT 1,
                                version DateTime64(3) DEFAULT now64(3)
                            ) ENGINE = ReplacingMergeTree(version)
                            ORDER BY name
                            """.trimIndent()
                        )
                    }
                }
                log.info("ClickHouse schema initialized")
                return
            } catch (e: Exception) {
                attempt++
                if (attempt >= maxAttempts) {
                    log.error("Failed to initialize ClickHouse after $maxAttempts attempts", e)
                    throw e
                }
                log.warn("ClickHouse not ready (attempt $attempt/$maxAttempts): ${e.message}")
                Thread.sleep(2000)
            }
        }
    }

    fun ping(): Boolean = try {
        connect().use { c ->
            c.createStatement().use { it.execute("SELECT 1") }
        }
        true
    } catch (e: Exception) {
        false
    }

    fun insertEvents(events: List<EventRow>) {
        if (events.isEmpty()) return
        val sql = "INSERT INTO kafka_events (topic, partition, offset, `key`, ts, kafka_ts, payload) VALUES (?, ?, ?, ?, ?, ?, ?)"
        connect().use { c ->
            c.autoCommit = false
            c.prepareStatement(sql).use { ps ->
                for (e in events) {
                    ps.setString(1, e.topic)
                    ps.setInt(2, e.partition)
                    ps.setLong(3, e.offset)
                    if (e.key == null) ps.setNull(4, java.sql.Types.VARCHAR) else ps.setString(4, e.key)
                    ps.setTimestamp(5, Timestamp.from(Instant.parse(e.ts)))
                    ps.setTimestamp(6, Timestamp.from(Instant.parse(e.kafkaTs)))
                    ps.setString(7, e.payload)
                    ps.addBatch()
                }
                ps.executeBatch()
            }
            c.commit()
        }
    }

    fun listTopics(activeOnly: Boolean = true): List<co.codeyogi.topicstore.model.TopicSpec> {
        val q = if (activeOnly)
            "SELECT name, toString(created_at), active FROM topic_registry FINAL WHERE active = 1 ORDER BY name"
        else
            "SELECT name, toString(created_at), active FROM topic_registry FINAL ORDER BY name"
        val out = mutableListOf<co.codeyogi.topicstore.model.TopicSpec>()
        connect().use { c ->
            c.createStatement().use { st ->
                st.executeQuery(q).use { rs ->
                    while (rs.next()) {
                        out.add(
                            co.codeyogi.topicstore.model.TopicSpec(
                                name = rs.getString(1),
                                createdAt = rs.getString(2),
                                active = rs.getInt(3) == 1,
                            )
                        )
                    }
                }
            }
        }
        return out
    }

    fun upsertTopic(name: String, active: Boolean) {
        val sql = "INSERT INTO topic_registry (name, active, version) VALUES (?, ?, now64(3))"
        connect().use { c ->
            c.prepareStatement(sql).use { ps ->
                ps.setString(1, name)
                ps.setInt(2, if (active) 1 else 0)
                ps.executeUpdate()
            }
        }
    }

    data class LookupFilter(
        val topic: String? = null,
        val key: String? = null,
        val from: String? = null,
        val to: String? = null,
        val jsonPath: String? = null,
        val jsonValue: String? = null,
        val limit: Int = 100,
        val offset: Int = 0,
    )

    fun lookup(f: LookupFilter): Pair<Long, List<EventRow>> {
        val where = mutableListOf<String>()
        val params = mutableListOf<Any?>()
        f.topic?.takeIf { it.isNotBlank() }?.let { where += "topic = ?"; params += it }
        f.key?.takeIf { it.isNotBlank() }?.let { where += "`key` = ?"; params += it }
        f.from?.takeIf { it.isNotBlank() }?.let { where += "ts >= parseDateTime64BestEffort(?)"; params += it }
        f.to?.takeIf { it.isNotBlank() }?.let { where += "ts <= parseDateTime64BestEffort(?)"; params += it }
        if (!f.jsonPath.isNullOrBlank() && !f.jsonValue.isNullOrBlank()) {
            where += "JSONExtractString(payload, ?) = ?"
            params += f.jsonPath
            params += f.jsonValue
        }
        val whereSql = if (where.isEmpty()) "" else " WHERE " + where.joinToString(" AND ")

        val rows = mutableListOf<EventRow>()
        var total = 0L
        connect().use { c ->
            c.prepareStatement("SELECT count() FROM kafka_events$whereSql").use { ps ->
                params.forEachIndexed { i, p -> ps.setObject(i + 1, p) }
                ps.executeQuery().use { rs -> if (rs.next()) total = rs.getLong(1) }
            }
            val sql = """
                SELECT topic, partition, offset, `key`, toString(ts), toString(kafka_ts), payload
                FROM kafka_events$whereSql
                ORDER BY ts DESC
                LIMIT ? OFFSET ?
            """.trimIndent()
            c.prepareStatement(sql).use { ps ->
                params.forEachIndexed { i, p -> ps.setObject(i + 1, p) }
                ps.setInt(params.size + 1, f.limit)
                ps.setInt(params.size + 2, f.offset)
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        rows += EventRow(
                            topic = rs.getString(1),
                            partition = rs.getInt(2),
                            offset = rs.getLong(3),
                            key = rs.getString(4),
                            ts = rs.getString(5),
                            kafkaTs = rs.getString(6),
                            payload = rs.getString(7),
                        )
                    }
                }
            }
        }
        return total to rows
    }
}
