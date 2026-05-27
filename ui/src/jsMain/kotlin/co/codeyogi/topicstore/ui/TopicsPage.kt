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

private val CARD = ClassName("card")
private val ROW = ClassName("row")
private val COL = ClassName("col")
private val GHOST = ClassName("ghost")
private val DANGER = ClassName("danger")
private val PILL_GREEN = ClassName("pill green")
private val PILL_RED = ClassName("pill red")
private val ERR = ClassName("error")

val TopicsPage = FC<Props> {
    var topics by useState<List<TopicSpec>>(emptyList())
    var newName by useState("")
    var error by useState<String?>(null)
    var loading by useState(false)

    fun refresh() {
        loading = true
        MainScope().launch {
            runCatching { Api.listTopics() }
                .onSuccess { topics = it; error = null }
                .onFailure { error = it.message }
            loading = false
        }
    }

    useEffectOnce { refresh() }

    div {
        className = CARD
        h2 { +"Add Topic" }
        div {
            className = ROW
            div {
                className = COL
                label { +"Kafka topic name" }
                input {
                    value = newName
                    placeholder = "events.orders"
                    onChange = { e -> newName = (e.target as HTMLInputElement).value }
                }
            }
            button {
                disabled = newName.isBlank()
                onClick = {
                    val n = newName.trim()
                    MainScope().launch {
                        runCatching { Api.addTopic(n) }
                            .onSuccess { newName = ""; error = null; refresh() }
                            .onFailure { error = it.message }
                    }
                }
                +"Add"
            }
            button {
                className = GHOST
                onClick = { refresh() }
                +"Refresh"
            }
        }
        error?.let { msg -> div { className = ERR; +"Error: $msg" } }
    }

    div {
        className = CARD
        h2 { +"Registered Topics (${topics.count { it.active }} active / ${topics.size} total)" }
        table {
            thead {
                tr {
                    th { +"Name" }
                    th { +"Status" }
                    th { +"Created" }
                    th { +"Actions" }
                }
            }
            tbody {
                if (topics.isEmpty()) {
                    tr { td { colSpan = 4; +(if (loading) "Loading..." else "No topics yet. Add one above.") } }
                }
                topics.forEach { t ->
                    tr {
                        key = t.name
                        td { +t.name }
                        td {
                            span {
                                className = if (t.active) PILL_GREEN else PILL_RED
                                +(if (t.active) "active" else "removed")
                            }
                        }
                        td { +(t.createdAt ?: "") }
                        td {
                            if (t.active) {
                                button {
                                    className = DANGER
                                    onClick = {
                                        MainScope().launch {
                                            runCatching { Api.deleteTopic(t.name) }
                                                .onSuccess { refresh() }
                                                .onFailure { error = it.message }
                                        }
                                    }
                                    +"Remove"
                                }
                            } else {
                                button {
                                    onClick = {
                                        MainScope().launch {
                                            runCatching { Api.addTopic(t.name) }
                                                .onSuccess { refresh() }
                                                .onFailure { error = it.message }
                                        }
                                    }
                                    +"Re-add"
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
