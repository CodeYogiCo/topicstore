package co.codeyogi.topicstore.ui

import kotlinx.coroutines.MainScope
import kotlinx.coroutines.launch
import react.FC
import react.Props
import react.dom.html.ReactHTML.button
import react.dom.html.ReactHTML.div
import react.dom.html.ReactHTML.h2
import react.dom.html.ReactHTML.input
import react.dom.html.ReactHTML.label
import react.dom.html.ReactHTML.option
import react.dom.html.ReactHTML.pre
import react.dom.html.ReactHTML.select
import react.dom.html.ReactHTML.span
import react.dom.html.ReactHTML.table
import react.dom.html.ReactHTML.tbody
import react.dom.html.ReactHTML.td
import react.dom.html.ReactHTML.th
import react.dom.html.ReactHTML.thead
import react.dom.html.ReactHTML.tr
import react.useEffectOnce
import react.useState
import web.cssom.ClassName
import web.html.HTMLInputElement
import web.html.HTMLSelectElement
import web.html.InputType

private val CARD = ClassName("card")
private val ROW = ClassName("row")
private val COL = ClassName("col")
private val ERR = ClassName("error")
private val MUTED = ClassName("muted")

val LookupPage = FC<Props> {
    var topics by useState<List<String>>(emptyList())
    var topic by useState("")
    var key by useState("")
    var from by useState("")
    var to by useState("")
    var jsonPath by useState("")
    var jsonValue by useState("")
    var limit by useState(50)
    var offset by useState(0)
    var loading by useState(false)
    var error by useState<String?>(null)
    var result by useState<LookupResponse?>(null)

    useEffectOnce {
        MainScope().launch {
            runCatching { Api.listTopics() }
                .onSuccess { topics = it.filter { t -> t.active }.map { t -> t.name } }
        }
    }

    fun runSearch() {
        loading = true
        MainScope().launch {
            runCatching {
                Api.lookup(topic, key, from, to, jsonPath, jsonValue, limit, offset)
            }.onSuccess { result = it; error = null }
             .onFailure { error = it.message }
            loading = false
        }
    }

    div {
        className = CARD
        h2 { +"Lookup" }
        div {
            className = ROW
            div {
                className = COL
                label { +"Topic" }
                select {
                    value = topic
                    onChange = { e -> topic = (e.target as HTMLSelectElement).value }
                    option { value = ""; +"(any)" }
                    topics.forEach { t -> option { value = t; +t } }
                }
            }
            div {
                className = COL
                label { +"Key (exact)" }
                input {
                    value = key
                    onChange = { e -> key = (e.target as HTMLInputElement).value }
                }
            }
            div {
                className = COL
                label { +"From (ISO)" }
                input {
                    value = from
                    placeholder = "2025-01-01T00:00:00Z"
                    onChange = { e -> from = (e.target as HTMLInputElement).value }
                }
            }
            div {
                className = COL
                label { +"To (ISO)" }
                input {
                    value = to
                    placeholder = "2026-01-01T00:00:00Z"
                    onChange = { e -> to = (e.target as HTMLInputElement).value }
                }
            }
        }
        div {
            className = ROW
            div {
                className = COL
                label { +"JSON path (e.g. user.id)" }
                input {
                    value = jsonPath
                    placeholder = "user.id"
                    onChange = { e -> jsonPath = (e.target as HTMLInputElement).value }
                }
            }
            div {
                className = COL
                label { +"JSON value (exact)" }
                input {
                    value = jsonValue
                    onChange = { e -> jsonValue = (e.target as HTMLInputElement).value }
                }
            }
            div {
                className = COL
                label { +"Limit" }
                input {
                    type = InputType.number
                    value = limit.toString()
                    onChange = { e -> limit = (e.target as HTMLInputElement).value.toIntOrNull() ?: 50 }
                }
            }
            div {
                className = COL
                label { +"Offset" }
                input {
                    type = InputType.number
                    value = offset.toString()
                    onChange = { e -> offset = (e.target as HTMLInputElement).value.toIntOrNull() ?: 0 }
                }
            }
            button {
                disabled = loading
                onClick = { runSearch() }
                +(if (loading) "Searching..." else "Search")
            }
        }
        error?.let { msg -> div { className = ERR; +"Error: $msg" } }
        span { className = MUTED; +"Tip: JSON path uses ClickHouse JSONExtractString syntax (e.g. nested.field)." }
    }

    result?.let { r ->
        div {
            className = CARD
            h2 { +"Results — ${r.rows.size} of ${r.total}" }
            table {
                thead {
                    tr {
                        th { +"Topic" }
                        th { +"Partition" }
                        th { +"Offset" }
                        th { +"Key" }
                        th { +"Kafka ts" }
                        th { +"Payload" }
                    }
                }
                tbody {
                    r.rows.forEach { row ->
                        tr {
                            key = "${row.topic}-${row.partition}-${row.offset}"
                            td { +row.topic }
                            td { +row.partition.toString() }
                            td { +row.offset.toString() }
                            td { +(row.key ?: "") }
                            td { +row.kafkaTs }
                            td { pre { +row.payload } }
                        }
                    }
                }
            }
        }
    }
}
