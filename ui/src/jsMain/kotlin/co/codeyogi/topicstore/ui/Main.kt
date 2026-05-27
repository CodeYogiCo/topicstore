package co.codeyogi.topicstore.ui

import react.create
import react.dom.client.createRoot
import web.dom.document

fun main() {
    val container = document.getElementById("root") ?: error("root not found")
    createRoot(container).render(App.create())
}
