package net.hanekawa.kafkakaiju

import org.apache.kafka.clients.admin.TopicDescription
import org.apache.kafka.common.Node

class KafkaClusterStateCache {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    private val nodes = HashMap<Int, Node>()
    private val topicDescriptions = HashMap<String, TopicDescription>()

    fun getNodeIds(): Collection<Int> {
        return nodes.keys
    }

    fun getNodeByIds(nodeIds: List<Int>): List<Node?> {
        val result = ArrayList<Node?>(nodeIds.size)
        nodeIds.forEachIndexed({ index, nodeId ->
            result[index] = nodes[nodeId]
        })
        return result
    }

    fun listTopicsNames(): Collection<String> {
        return topicDescriptions.keys
    }

    fun getTopicDescriptions(topicNames: Collection<String>): List<TopicDescription?> {
        return topicNames.map {
            topicDescriptions[it]
        }
    }

    fun updateNodes(newNodes: Collection<Node>?) {
        synchronized(nodes) {
            nodes.clear()
            newNodes?.forEach({
                nodes[it.id()] = it
            })
        }
    }

    fun updateTopicDescriptions(newTopicDescriptions: Collection<TopicDescription>) {
        synchronized(topicDescriptions) {
            topicDescriptions.clear()
            newTopicDescriptions.forEach({
                topicDescriptions[it.name()] = it
            })
        }
    }
}