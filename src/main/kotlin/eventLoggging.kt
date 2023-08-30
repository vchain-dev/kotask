package com.zamna.kotask.eventLogging

import mu.withLoggingContext
import org.slf4j.Logger


public inline fun <T> withLoggingAction(
    event: String,
    body: () -> T
): T {
    return withLoggingContext("action" to event, restorePrevious = true, body = body)
}

public inline fun <T> loggingAction(
    event: String,
    body: () -> T
): T {
    return withLoggingContext("action" to event, restorePrevious = true, body = body)
}

fun Logger.cTrace(message: String, vararg context: Pair<String, String?>) =
    withLoggingContext(mapOf(*context)) {
        this.trace(message)
    }

fun Logger.cDebug(message: String, vararg context: Pair<String, String?>) =
    withLoggingContext(mapOf(*context)) {
        this.debug(message)
    }

fun Logger.cInfo(message: String, vararg context: Pair<String, String?>) =
    withLoggingContext(mapOf(*context)) {
        this.info(message)
    }

fun Logger.cWarn(message: String, vararg context: Pair<String, String?>) =
    withLoggingContext(mapOf(*context)) {
        this.warn(message)
    }

fun Logger.cError(message: String, t: Throwable, vararg context: Pair<String, String?>) =
    withLoggingContext(mapOf(*context)) {
        this.error(message, t)
    }

object AddContextParams {
    fun action(value: String) = "action" to value
}