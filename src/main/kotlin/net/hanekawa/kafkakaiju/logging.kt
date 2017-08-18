package net.hanekawa.kafkakaiju

import org.slf4j.Logger
import org.slf4j.LoggerFactory

fun <T : Any> getLogger(ofClass: Class<T>): Logger {
    return LoggerFactory.getLogger(unwrapCompanionClass(ofClass).name)
}

private fun <T : Any> unwrapCompanionClass(ofClass: Class<T>): Class<*> {
    return if (ofClass.enclosingClass != null) {
        ofClass.enclosingClass
    } else {
        ofClass
    }
}