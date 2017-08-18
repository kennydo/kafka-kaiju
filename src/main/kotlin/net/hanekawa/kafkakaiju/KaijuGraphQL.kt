package net.hanekawa.kafkakaiju

import graphql.ExecutionResult
import graphql.GraphQL
import graphql.schema.idl.RuntimeWiring
import graphql.schema.idl.SchemaGenerator
import graphql.schema.idl.SchemaParser

class KaijuGraphQL(val clusterStateManager: ClusterStateManager) {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    private val schemaParser = SchemaParser()
    private val schemaGenerator = SchemaGenerator()

    private val schemaFile = this::class.java.getResource("../../../kafkakaiju.graphqls").readText()

    private val typeRegistry = schemaParser.parse(schemaFile)
    private val wiring = buildRuntimeWiring()
    private val graphQLSchema = schemaGenerator.makeExecutableSchema(typeRegistry, wiring)

    private val graphQL = GraphQL.newGraphQL(graphQLSchema).build()

    fun execute(requestString: String, operationName: String? = null, arguments: Map<String, Any>? = null): ExecutionResult {
        return graphQL.execute(requestString, operationName, null, arguments ?: emptyMap())
    }

    private fun buildRuntimeWiring(): RuntimeWiring {
        return RuntimeWiring.newRuntimeWiring()
                .type("QueryType", {
                    it.dataFetcher("brokers", { environment ->
                        data class Foo(val id: Int, val host: String, val port: Int)

                        KafkaKaijuApp.LOG.info("environment arguments: " + environment.arguments)
                        KafkaKaijuApp.LOG.info("environment fields: " + environment.fields)

                        arrayListOf(Foo(3, "localhost", 9092))
                    })
                })
                .build()
    }
}