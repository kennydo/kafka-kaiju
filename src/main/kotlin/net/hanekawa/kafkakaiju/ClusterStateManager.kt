package net.hanekawa.kafkakaiju

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ListTopicsOptions
import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.Node
import java.util.*
import kotlin.concurrent.thread

class ClusterStateManager(private val stateCache: ClusterStateCache, private val adminClient: AdminClient) : Runnable, AutoCloseable {
    companion object {
        val LOG = getLogger(this::class.java)

        fun create(kafkaBootstrapServers: String, kafkaClientId: String): ClusterStateManager {
            val cache = ClusterStateCache()

            val adminProperties = Properties()
            adminProperties.setProperty("bootstrap.servers", kafkaBootstrapServers)
            adminProperties.setProperty("client.id", kafkaClientId)

            val adminClient = AdminClient.create(adminProperties)

            return ClusterStateManager(
                    stateCache = cache,
                    adminClient = adminClient
            )
        }
    }

    override fun run() {
        thread(start = true) {
            while (true) {
                refreshCache()
                Thread.sleep(60_000)
            }
        }
    }

    override fun close() {
        LOG.info("Closing admin client")
        adminClient.close()
        LOG.info("Finished closing")
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

        try {
            KafkaFuture.allOf(updateClusterFuture, updateTopicDescriptions).get()
        } catch (e: java.util.concurrent.ExecutionException) {
            // Thanks https://github.com/apache/kafka/pull/3584
            LOG.error("Exception while updating topic descriptions", e)
        }
        LOG.info("Finished refreshing cache")
    }
}

