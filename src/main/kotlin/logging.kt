import io.github.oshai.kotlinlogging.KLogger
import io.github.oshai.kotlinlogging.withLoggingContext
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.ThreadContextElement
import org.slf4j.MDC
import java.util.*
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

// region coroutine utils
fun loggingExceptionHandler(logger: KLogger) = CoroutineExceptionHandler { _, throwable ->
    logger.errorWithExceptionContext("unhandled exception in coroutine", throwable)
}

fun loggingScope(logger: KLogger) = CoroutineScope(loggingExceptionHandler(logger))

fun supervisorLoggingScope(logger: KLogger) = CoroutineScope(
    SupervisorJob() + loggingExceptionHandler(logger)
)
// endregion


// region MDC utils
private val Throwable.exceptionChain: Sequence<Throwable>
    get() = generateSequence(this) { e -> e.cause?.takeIf { it != e } }

/**
 * Get merged context from all exceptions in the chain
 */
internal fun Throwable.getLogCtx(): Map<String, String>? = this.exceptionChain
    // get context for each exception in the chain
    .mapNotNull { exceptionContextMap[it] }
    // merge contexts from all exceptions in the chain
    // with priority to the most nested exception
    .reduceOrNull { acc, ctx -> acc + ctx }

val exceptionContextMap: MutableMap<Throwable, Map<String, String>> =
    Collections.synchronizedMap(WeakHashMap())

fun KLogger.errorWithExceptionContext(msg: String, e: Throwable) {
    e.getLogCtx()?.let { ctx ->
        withLoggingContext(ctx) {
            this.error(msg, e)
        }
    } ?: this.error(msg, e)
}

fun KLogger.errorWithExceptionContext(e: Throwable, msg: () -> String) {
    this.errorWithExceptionContext(msg(), e)
}

inline fun <T> withLogCtx(
    pair: Pair<String, String?>,
    restorePrevious: Boolean = true,
    body: () -> T,
): T = withLoggingContext(pair, restorePrevious) {
    try {
        body()
    } catch (e: Throwable) {
        exceptionContextMap.putIfAbsent(e, MDC.getCopyOfContextMap())
        throw e
    }
}

inline fun <T> withLogCtx(
    map: Map<String, String?>,
    restorePrevious: Boolean = true,
    body: () -> T,
): T = withLoggingContext(map, restorePrevious) {
    try {
        body()
    } catch (e: Throwable) {
        exceptionContextMap.putIfAbsent(e, MDC.getCopyOfContextMap())
        throw e
    }
}

inline fun <T> withLogCtx(
    vararg pair: Pair<String, String?>,
    restorePrevious: Boolean = true,
    body: () -> T,
): T = withLoggingContext(
    pair = pair,
    restorePrevious = restorePrevious,
) {
    try {
        body()
    } catch (e: Throwable) {
        exceptionContextMap.putIfAbsent(e, MDC.getCopyOfContextMap())
        throw e
    }
}
// endregion

typealias MDCContextMap = Map<String, String?>

class MDCContext(
    /**
     * The value of [MDC] context map.
     */
    @Suppress("MemberVisibilityCanBePrivate")
    var contextMap: MDCContextMap? = MDC.getCopyOfContextMap()
) : ThreadContextElement<MDCContextMap>, AbstractCoroutineContextElement(Key) {
    /**
     * Key of [MDCContext] in [CoroutineContext].
     */
    companion object Key : CoroutineContext.Key<MDCContext>

    /** @suppress */
    override fun updateThreadContext(context: CoroutineContext): MDCContextMap {
        val oldState = MDC.getCopyOfContextMap()
        setCurrent(contextMap)
        return oldState ?: emptyMap()
    }

    /** @suppress */
    override fun restoreThreadContext(context: CoroutineContext, oldState: MDCContextMap) {
        contextMap = MDC.getCopyOfContextMap()
        setCurrent(oldState)
    }

    private fun setCurrent(contextMap: MDCContextMap?) {
        if (contextMap == null) {
            MDC.clear()
        } else {
            MDC.setContextMap(contextMap)
        }
    }
}
