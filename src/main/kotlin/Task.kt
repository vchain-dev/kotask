package com.zamna.kotask

import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.oshai.kotlinlogging.withLoggingContext
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import withLogCtx
import java.util.*
import kotlin.time.Duration

class TaskCallFactory<T: Any>(val task: Task<T>, val input: T, val manager: TaskManager) {
    operator fun invoke(params: CallParams): TaskCall = task.createTaskCall(input, params, manager)
}

@Serializable
object NoInput

object TaskEvents {
    val MESSAGE_RECEIVED: String = "MESSAGE_RECEIVED"
    val MESSAGE_SENT: String = "MESSAGE_SENT"
    val MESSAGE_SUBMIT_RETRY: String = "MESSAGE_SUBMIT_RETRY"
    val MESSAGE_FAIL: String = "MESSAGE_FAIL"
    val MESSAGE_FAIL_RETRY: String = "MESSAGE_FAIL_RETRY"
    val MESSAGE_FAIL_NO_RETRY: String = "MESSAGE_FAIL_NO_RETRY"
    val MESSAGE_COMPLETE: String = "MESSAGE_COMPLETE"
}

class Task<T: Any> @PublishedApi internal constructor(
    private val inputSerializer: KSerializer<T>,
    val name: String,
    val retry: IRetryPolicy? = null,
    val handler: TaskHandler<T>
) {
    private var logger = KotlinLogging.logger { }

    companion object {
        inline fun <reified T : Any> create(
            name: String, retry: IRetryPolicy? = null, noinline handler: TaskHandler<T>
        ) = Task(serializer(), name, retry, handler)

        inline fun <reified T : Any> create(
            name: String, retry: IRetryPolicy? = null, noinline handler: OnlyInputTaskHandler<T>
        ) = create(name, retry, handler.toTaskHandler())

        fun create(
            name: String, retry: IRetryPolicy? = null, handler: NoArgTaskHandler
        ) = create(name, retry, handler.toTaskHandler())
    }

    fun callLater(input: T, params: CallParams = CallParams(), manager: TaskManager = TaskManager.getDefaultInstance()) {
        val logCtx = mapOf(
            "delay" to params.delay.toString(),
            "callId" to params.callId,
            "attemptNum" to params.attemptNum.toString(),
            "action" to TaskEvents.MESSAGE_SENT
        )
        withLogCtx(logCtx) {
            val inputStr = Json.encodeToString(inputSerializer, input)
            manager.enqueueTaskCall(this, inputStr, params)
            logger.atInfo {
                message = "Call task $name later with input $inputStr"
                payload = logCtx
            }
        }
    }

    fun prepareInput(input: T, manager: TaskManager = TaskManager.getDefaultInstance()): TaskCallFactory<T> {
        return TaskCallFactory(this, input, manager)
    }

    fun prepareInput(manager: TaskManager = TaskManager.getDefaultInstance()): TaskCallFactory<NoInput> {
        return TaskCallFactory(this as Task<NoInput>, NoInput, manager)
    }

    fun createTaskCall(input: T, params: CallParams = CallParams(), manager: TaskManager = TaskManager.getDefaultInstance()): TaskCall {
        val inputStr = Json.encodeToString(inputSerializer, input)
        return manager.createTaskCall(this, inputStr, params)
    }

    suspend fun execute(inputStr: String, params: CallParams, manager: TaskManager) {
        val logCtx = mapOf(
            "task" to name,
            "callId" to params.callId,
            "delay" to params.delay.toString(),
            "attemptNum" to params.attemptNum.toString()
        )
        withLogCtx(logCtx) {
            val ctx = ExecutionContext(params, manager, logCtx = logCtx)

            logger.atDebug {
                message = "Execute task $name with input $inputStr"
                payload = ctx.logCtx
            }

            // TODO: what to do with deserialization errors?
            val input  = Json.decodeFromString(inputSerializer, inputStr)
            withLogCtx("action" to TaskEvents.MESSAGE_RECEIVED) {
                logger.atInfo {
                    message = "Start task with name=$name callId=${params.callId} with $inputStr"
                    payload = mapOf(
                        "action" to TaskEvents.MESSAGE_RECEIVED
                    )
                }
            }

            try {
                handler(ctx, input)

                withLogCtx("action" to TaskEvents.MESSAGE_COMPLETE) {
                    logger.atInfo {
                        message = "Complete task $name with callId=${params.callId} with $inputStr"
                        payload = mapOf(
                            "action" to TaskEvents.MESSAGE_COMPLETE
                        )
                    }
                }
            } catch (e: RepeatTask) {
                withLogCtx("action" to TaskEvents.MESSAGE_SUBMIT_RETRY) {
                    logger.atInfo {
                        message = "Received RepeatTask from task $name with callId=${params.callId} with $inputStr"
                        payload = mapOf(
                            "action" to TaskEvents.MESSAGE_SUBMIT_RETRY
                        )
                    }
                    manager.enqueueTaskCall(this, inputStr, e.getRetryCallParams(params))
                }
            } catch (e: ForceRetry) {
                withLogCtx("action" to TaskEvents.MESSAGE_SUBMIT_RETRY) {
                    logger.atInfo {
                        message = "Received ForceRetry from task $name with callId=${params.callId} with $inputStr"
                        payload = mapOf(
                            "action" to TaskEvents.MESSAGE_SUBMIT_RETRY
                        )
                    }
                    manager.enqueueTaskCall(this, inputStr, e.getRetryCallParams(params))
                }
            } catch (e: FailNoRetry) {
                withLogCtx("action" to TaskEvents.MESSAGE_FAIL_NO_RETRY) {
                    logger.atInfo {
                        message = "Received FailNoRetry from task $name with callId=${params.callId} with $inputStr"
                        payload = mapOf(
                            "action" to TaskEvents.MESSAGE_FAIL_NO_RETRY
                        )
                    }
                }
            } catch (e: Throwable) {
                withLogCtx("action" to TaskEvents.MESSAGE_FAIL) {
                    logger.atError {
                        message = "Task $name failed with callId=${params.callId} with $inputStr"
                        cause = e
                        payload = mapOf(
                            "action" to TaskEvents.MESSAGE_FAIL
                        )
                    }
                }

                if (getRetryPolicy(manager).shouldRetry(params)) {
                    withLogCtx("action" to TaskEvents.MESSAGE_FAIL_RETRY) {
                        logger.atInfo {
                            message = "Retry task $name with callId=${params.callId} with $inputStr"
                            cause = e
                            payload = mapOf(
                                "action" to TaskEvents.MESSAGE_FAIL_RETRY
                            )
                        }
                        manager.enqueueTaskCall(this, inputStr, getRetryPolicy(manager).getNextRetryCallParams(params))
                    }
                } else {
                    withLogCtx("action" to TaskEvents.MESSAGE_FAIL_NO_RETRY) {
                        logger.atError {
                            message = "Task $name failed with callId=${params.callId} with $inputStr and no more retries left"
                            cause = e
                            payload = mapOf(
                                "action" to TaskEvents.MESSAGE_FAIL_NO_RETRY
                            )
                        }
                    }
                }
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

typealias TaskHandler<T> = (suspend (ctx: ExecutionContext, input: T) -> Unit)
typealias OnlyInputTaskHandler<T> = (suspend (input: T) -> Unit)
typealias NoArgTaskHandler = (suspend () -> Unit)

fun <T> OnlyInputTaskHandler<T>.toTaskHandler(): TaskHandler<T> {
    return { _: ExecutionContext, input: T ->
        this(input)
    }
}

fun NoArgTaskHandler.toTaskHandler(): TaskHandler<NoInput> {
    return { _: ExecutionContext, _: Any ->
        this()
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
    init {
        if (delay < Duration.ZERO) throw IllegalArgumentException("delay")
    }

    companion object {
        fun rndId() = UUID.randomUUID().toString()
    }

    fun nextAttempt() = copy(attemptNum = attemptNum + 1)
}

