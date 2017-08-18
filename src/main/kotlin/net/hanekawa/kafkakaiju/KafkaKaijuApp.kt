package net.hanekawa.kafkakaiju

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import org.jetbrains.ktor.host.embeddedServer
import org.jetbrains.ktor.http.ContentType
import org.jetbrains.ktor.netty.Netty
import org.jetbrains.ktor.response.respondText
import org.jetbrains.ktor.routing.get
import org.jetbrains.ktor.routing.routing
import java.util.*
import kotlin.concurrent.thread

class KafkaKaijuApp(val config: Config) {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    fun run() {
        val adminProperties = Properties()
        adminProperties.setProperty("bootstrap.servers", config.kafkaBootstrapServers)
        adminProperties.setProperty("client.id", config.kafkaClientId)

        val adminClient = AdminClient.create(adminProperties)

        val cache = KafkaClusterStateCache()
        val updater = StateCacheUpdater(cache, adminClient)

        updater.run()

        val server = embeddedServer(Netty, 8080) {
            routing {
                get("/topics") {
                    call.respondText(cache.listTopicsNames().toString(), ContentType.Text.Plain)
                }
                get("/partitions") {
                    call.respondText(cache.getTopicDescriptions(cache.listTopicsNames()).toString(), ContentType.Text.Plain)
                }
            }
        }
        server.start(wait = true)

        adminClient.close()
        System.out.println("Closed")
    }
}


class StateCacheUpdater(val stateCache: KafkaClusterStateCache, val adminClient: AdminClient) : Runnable {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    override fun run() {
        thread(start = true) {
            while (true) {
                refreshCache()
                Thread.sleep(60_000)
            }
        }
    }

    private fun refreshCache() {
        LOG.info("Refreshing cache")
        val updateClusterFuture = adminClient.describeCluster().nodes().thenApply(object : KafkaFuture.Function<Collection<Node>, Unit>() {
            override fun apply(nodes: Collection<Node>?) {
                LOG.info("Updating nodes")
                stateCache.updateNodes(nodes)
            }
        })

        val topicNames = adminClient.listTopics(ListTopicsOptions().listInternal(true)).listings().thenApply(
                object : KafkaFuture.Function<Collection<TopicListing>, Collection<String>>() {
                    override fun apply(topics: Collection<TopicListing>?): Collection<String> {
                        LOG.info("Fetched {} topics", topics?.size)
                        return topics?.map { it.name() } ?: emptyList()
                    }
                }
        ).get()

        val updateTopicDescriptions = adminClient.describeTopics(topicNames).all().thenApply(object : KafkaFuture.Function<Map<String, TopicDescription>, Unit>() {
            override fun apply(topicDescriptionMapping: Map<String, TopicDescription>) {
                LOG.info("Updating {} topic descriptions", topicDescriptionMapping.size)
                stateCache.updateTopicDescriptions(topicDescriptionMapping.values)
            }
        })

        KafkaFuture.allOf(updateClusterFuture, updateTopicDescriptions).get()
        LOG.info("Finished refreshing cache")
    }
}


fun main(args: Array<String>) {
    val config = Config.load()
    val app = KafkaKaijuApp(config)
    app.run()
}