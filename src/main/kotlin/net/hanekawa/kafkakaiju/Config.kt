package net.hanekawa.kafkakaiju

import com.typesafe.config.ConfigFactory
import io.github.config4k.extract

data class Config(
        val kafkaBootstrapServers: String,
        val kafkaClientId: String) {

    companion object {
        fun load(): Config {
            val config = ConfigFactory.load()
            return Config(
                    kafkaBootstrapServers = config.extract<String>("kafkaBootstrapServers"),
                    kafkaClientId = config.extract<String>("kafkaClientId")
            )
        }
    }
}