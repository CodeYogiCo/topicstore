package co.codeyogi.topicstore.api

import co.codeyogi.topicstore.model.CreateTopicRequest
import co.codeyogi.topicstore.model.TopicSpec
import co.codeyogi.topicstore.registry.TopicRegistry
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.*

@Controller("/api/topics")
class TopicsController(private val registry: TopicRegistry) {

    @Get
    fun list(): List<TopicSpec> = registry.list()

    @Post
    fun create(@Body req: CreateTopicRequest): HttpResponse<TopicSpec> {
        return try {
            registry.add(req.name)
            HttpResponse.created(TopicSpec(name = req.name, active = true))
        } catch (e: IllegalArgumentException) {
            HttpResponse.badRequest()
        }
    }

    @Delete("/{name}")
    fun delete(@PathVariable name: String): HttpResponse<Unit> {
        registry.remove(name)
        return HttpResponse.status(HttpStatus.NO_CONTENT)
    }
}
