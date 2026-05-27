package co.codeyogi.topicstore.ui

import kotlinx.coroutines.MainScope
import kotlinx.coroutines.launch
import react.FC
import react.Props
import react.dom.html.ReactHTML.a
import react.dom.html.ReactHTML.div
import react.dom.html.ReactHTML.main
import react.dom.html.ReactHTML.nav
import react.dom.html.ReactHTML.span
import react.useEffectOnce
import react.useState
import web.cssom.ClassName

enum class Page { TOPICS, LOOKUP }

private val ACTIVE = ClassName("active")
private val PILL = ClassName("pill")
private val PILL_GREEN = ClassName("pill green")
private val PILL_RED = ClassName("pill red")
private val SPACER = ClassName("spacer")

val App = FC<Props> {
    var page by useState(Page.TOPICS)
    var health by useState<HealthResponse?>(null)

    useEffectOnce {
        MainScope().launch {
            runCatching { Api.health() }.onSuccess { health = it }
        }
    }

    nav {
        a {
            if (page == Page.TOPICS) className = ACTIVE
            onClick = { page = Page.TOPICS }
            href = "#"
            +"Topics"
        }
        a {
            if (page == Page.LOOKUP) className = ACTIVE
            onClick = { page = Page.LOOKUP }
            href = "#"
            +"Lookup"
        }
        div { className = SPACER }
        health?.let { h ->
            span {
                className = if (h.status == "UP") PILL_GREEN else PILL_RED
                +"backend: ${h.status}"
            }
            span { className = PILL; +"clickhouse: ${if (h.clickhouse) "ok" else "down"}" }
            span { className = PILL; +"kafka: ${if (h.kafka) "ok" else "down"}" }
            span { className = PILL; +"active topics: ${h.activeTopics.size}" }
        }
    }

    main {
        when (page) {
            Page.TOPICS -> TopicsPage()
            Page.LOOKUP -> LookupPage()
        }
    }
}
