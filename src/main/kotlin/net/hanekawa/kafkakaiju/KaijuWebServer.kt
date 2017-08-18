package net.hanekawa.kafkakaiju

import com.squareup.moshi.FromJson
import com.squareup.moshi.JsonDataException
import com.squareup.moshi.Moshi
import com.squareup.moshi.ToJson
import graphql.GraphQLError
import org.jetbrains.ktor.host.embeddedServer
import org.jetbrains.ktor.http.ContentType
import org.jetbrains.ktor.http.HttpStatusCode
import org.jetbrains.ktor.netty.Netty
import org.jetbrains.ktor.request.receiveText
import org.jetbrains.ktor.response.respondText
import org.jetbrains.ktor.routing.post
import org.jetbrains.ktor.routing.route
import org.jetbrains.ktor.routing.routing
import java.io.Closeable
import java.util.concurrent.TimeUnit


data class GraphQLRequest(val query: String, val operationName: String?, val variables: Map<String, Any>?)
data class GraphQLResponse(val data: Map<String, Any>?, val errors: List<GraphQLError>?)
data class ErrorLocation(val line: Int, val column: Int)
data class ErrorEntry(val message: String, val locations: List<ErrorLocation>?)


class KaijuWebServer(private val graphQL: KaijuGraphQL, private val port: Int = 8080) : Runnable, Closeable {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    private val moshi = Moshi
            .Builder()
            .add(object {
                @ToJson
                fun toErrorEntry(error: GraphQLError): ErrorEntry {
                    return ErrorEntry(
                            message = error.message,
                            locations = error.locations?.map {
                                ErrorLocation(it.line, it.column)
                            }
                    )
                }

                @FromJson
                fun fromErrorEntry(entry: ErrorEntry): GraphQLError {
                    throw NotImplementedError("Not implemented!")
                }
            })
            .build()
    private val graphQLRequestAdapter = moshi.adapter(GraphQLRequest::class.java).failOnUnknown()
    private val graphQLResponseAdapter = moshi.adapter(GraphQLResponse::class.java)

    private val server = embeddedServer(Netty, port) {
        routing {
            route("/api/v1") {
                post("graphql") {
                    val rawBody = call.receiveText()
                    val request = try {
                        graphQLRequestAdapter.fromJson(rawBody)
                    } catch (e: JsonDataException) {
                        LOG.error("Unable to parse JSON: {}", rawBody)
                        null
                    }

                    if (request == null) {
                        call.respondText("Invalid GraphQL query", ContentType.Text.Plain, HttpStatusCode.BadRequest)
                        return@post
                    }

                    LOG.info("Got request: {}", request)

                    val executionResult = graphQL.execute(request.query, request.operationName, request.variables)

                    val resultData = executionResult.getData<Map<String, Any>>()
                    val resultErrors = executionResult.errors

                    if (resultErrors.size != 0) {
                        LOG.info("Execution generated errors: {}", resultErrors)
                    }

                    val response = GraphQLResponse(
                            resultData,
                            if (resultErrors.size == 0) {
                                null
                            } else {
                                resultErrors
                            }
                    )

                    call.respondText(graphQLResponseAdapter.toJson(response), ContentType.Application.Json)
                }
            }
        }
    }

    override fun run() {
        server.start(wait = true)
    }

    override fun close() {
        server.stop(30, 30, TimeUnit.SECONDS)
    }
}