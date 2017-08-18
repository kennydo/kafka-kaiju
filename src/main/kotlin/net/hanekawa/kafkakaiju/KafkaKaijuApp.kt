package net.hanekawa.kafkakaiju


class KafkaKaijuApp(val config: Config) {
    companion object {
        val LOG = getLogger(this::class.java)
    }

    fun run() {
        var clusterStateManager = ClusterStateManager.create(config.kafkaBootstrapServers, config.kafkaClientId)

        clusterStateManager.use {
            clusterStateManager.run()

            val graphQL = KaijuGraphQL(clusterStateManager)

            val webServer = KaijuWebServer(graphQL)

            webServer.use {
                webServer.run()
            }
        }
        System.out.println("Finished!")
    }
}

fun main(args: Array<String>) {
    val config = Config.load()
    val app = KafkaKaijuApp(config)
    app.run()
}