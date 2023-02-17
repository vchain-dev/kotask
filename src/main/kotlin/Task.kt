package com.zamna.kotask

import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.time.Duration


class Task<T: Any> @PublishedApi internal constructor(
    private val inputSerializer: KSerializer<T>,
    val name: String,
    val retry: IRetryPolicy? = null,
    val handler: TaskHandlerWithContext<T>
) {
    private var logger = LoggerFactory.getLogger(this::class.java)

    companion object {
        inline fun <reified T : Any> create(
            name: String, retry: IRetryPolicy? = null, noinline handler: TaskHandlerWithContext<T>
        ) = Task(serializer(), name, retry, handler)

        inline fun <reified T : Any> create(
            name: String, retry: IRetryPolicy? = null, noinline handler: TaskHandler<T>
        ) = create(name, retry, handler.toWithContext())
    }

    fun callLater(input: T, params: CallParams = CallParams(), manager: TaskManager = TaskManager.getDefaultInstance()) {
        val inputStr = Json.encodeToString(inputSerializer, input)
        logger.debug("Call task $name later with input $inputStr")
        manager.enqueueTaskCall(this, inputStr, params)
    }

    fun createTaskCall(input: T, params: CallParams = CallParams(), manager: TaskManager = TaskManager.getDefaultInstance()): TaskCall {
        val inputStr = Json.encodeToString(inputSerializer, input)
        logger.debug("Make task call for $name with input $inputStr")
        return manager.createTaskCall(this, inputStr, params)
    }

    suspend fun execute(inputStr: String, params: CallParams, manager: TaskManager) {
        logger.debug("Execute task $name with input $inputStr")
        // TODO: what to do with deserialization errors?
        var  input  = Json.decodeFromString(inputSerializer, inputStr)
        val ctx = ExecutionContext(
            params, manager, logCtx = mapOf(
                "task" to name,
                "callId" to params.callId,
            )
        )
        logger.info("Start task $name with callId=${params.callId}")
        try {
            handler(ctx, input)
        } catch (e: RepeatTask) {
            logger.info("Received RepeatTask from task $name with callId=${params.callId}")
            manager.enqueueTaskCall(this, inputStr, e.getRetryCallParams(params))
        } catch (e: ForceRetry) {
            logger.info("Received ForceRetry from task $name with callId=${params.callId}")
            manager.enqueueTaskCall(this, inputStr, e.getRetryCallParams(params))
        } catch (e: FailNoRetry) {
            logger.info("Received NoRetry from task $name with callId=${params.callId}")
        } catch (e: Exception) {
            logger.error("Task $name failed with callId=${params.callId}", e)
            if (getRetryPolicy(manager).shouldRetry(params)) {
                logger.info("Retry task $name with callId=${params.callId}")
                manager.enqueueTaskCall(this, inputStr, getRetryPolicy(manager).getNextRetryCallParams(params))
            } else {
                logger.error("Task $name failed with callId=${params.callId} and no more retries left")
            }

        }
    }

    private fun getRetryPolicy(manager: TaskManager): IRetryPolicy {
        return retry ?: manager.defaultRetryPolicy
    }
}

@Serializable
data class TaskCall(
    val taskName: String,
    val message: Message,
) {
    fun callLater(manager: TaskManager = TaskManager.getDefaultInstance()) {
        manager.enqueueTaskCall(this)
    }

}

typealias TaskHandlerWithContext<T> = (suspend (ctx: ExecutionContext, input: T) -> Unit)
typealias TaskHandler<T> = (suspend (input: T) -> Unit)

fun <T> TaskHandler<T>.toWithContext(): TaskHandlerWithContext<T> {
    return { _: ExecutionContext, input: T ->
        this(input)
    }
}

data class ExecutionContext(
    val callParams: CallParams,
    val taskManager: TaskManager,
    val logCtx: Map<String, String> = emptyMap()
)

data class CallParams(
    val callId: String = rndId(),
    val delay: Duration = Duration.ZERO,
    val attemptNum: Int = 1
) {
    companion object {
        fun rndId() = UUID.randomUUID().toString()
    }

    fun nextAttempt() = copy(attemptNum = attemptNum + 1)
}

