package com.zamna.kotask

import MDCContext
import Settings
import kotlinx.coroutines.*
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import kotlinx.serialization.Serializable
import cleanScheduleWorker
import io.github.oshai.kotlinlogging.KotlinLogging
import loggingScope
import withLogCtx
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.toDuration

// TODO(baitcode): TaskManager is getting huge
class TaskManager(
    private val broker: IMessageBroker,
    val scheduler: IScheduleTracker = InMemoryScheduleTracker(),
    private val queueNamePrefix: String = "kotask-",
    val defaultRetryPolicy: IRetryPolicy = RetryPolicy(4.seconds, 20, expBackoff = true, maxDelay = 1.hours),
    schedulersScope: CoroutineScope? = null,
): AutoCloseable {
    private val knownTasks: MutableMap<String, Task<*>> = mutableMapOf()
    // TODO(baitcode): Why use list? When we always have single consumer. Is it for concurrency.
    private val tasksConsumers: MutableMap<String, MutableList<IConsumer>> = mutableMapOf()
    private val tasksSchedulers: MutableMap<String, Job> = mutableMapOf()
    private var logger = KotlinLogging.logger {  }
    private val scope = schedulersScope ?: loggingScope(logger)

    fun knownSchedulerNames() = tasksSchedulers.keys.toSet()
    fun knownWorkerNames() = knownTasks.keys.toSet()

    fun enqueueTaskCall(task: Task<*>, inputStr: String, params: CallParams) {
        checkTaskUniq(task)
        val call = createTaskCall(task, inputStr, params)
        enqueueTaskCall(call)
    }

    fun enqueueTaskCall(call: TaskCall) {
        withLogCtx(
            "callId" to (call.message.headers["call-id"] ?: "unknown"),
            "taskName" to call.taskName,
            "delayMs" to call.message.delayMs.toString()
        ) {
            logger.debug { "Enqueue task call" }
            broker.submitMessage(queueNameByTaskName(call.taskName), call.message)

            if (broker is LocalBroker && !tasksConsumers.containsKey(call.taskName)) {
                // Start worker for local broker
                startWorker(knownTasks[call.taskName]!!)
            }
        }
    }

    fun createTaskCall(task: Task<*>, inputStr: String, params: CallParams): TaskCall {
        checkTaskUniq(task)
        return TaskCall(task.name, paramsToMessage(inputStr, params))
    }

    private fun startWorker(task: Task<*>) {
        withLogCtx("taskName" to task.name) {
            logger.info { "Starting worker for task ${task.name}" }
            checkTaskUniq(task)
            tasksConsumers.getOrDefault(task.name, mutableListOf()).add(
                broker.startConsumer(queueNameByTaskName(task.name)) { message: Message, ack: () -> Any ->
                    task.execute(message.body.decodeToString(), messageToParams(message), this)
                    ack()
                }
            )
        }
    }

    fun <T: Any> startScheduler(
        workloadName: String, schedulePolicy: IRepeatingSchedulePolicy, taskCallFactory: TaskCallFactory<T>
    ) {
        // TODO(baitcode): unclear how to test that schedule cleaner starts
        // Clean schedule worker start
        tasksSchedulers.getOrPut(cleanScheduleWorkloadName) {
            scope.launch(MDCContext()) {
                while (true) {
                    try {
                        everyHour
                            .getNextCalls()
                            .takeWhile { it < Clock.System.now() + Settings.schedulingHorizon }
                            .forEach { date ->
                                submitScheduleMessage(
                                    cleanScheduleWorkloadName,
                                    date,
                                    cleanScheduleWorker.prepareInput()
                                )
                            }
                    } catch (e: Exception) {
                        if (e is CancellationException) {
                            // catching CancellationException leads to dead loop
                            throw e
                        }
                        logger.error(e) { "Error in scheduler for $cleanScheduleWorkloadName" }
                    }
                    delay(Settings.scheduleDelayDuration)
                }
            }
        }

        withLogCtx("taskName" to workloadName) {
            tasksSchedulers.getOrPut(workloadName) {
                scope.launch(MDCContext()) {
                    logger.info { "Launching scheduler for $workloadName" }
                    while (true) {
                        try {
                            schedulePolicy
                                .getNextCalls()
                                .takeWhile { it < Clock.System.now() + Settings.schedulingHorizon }
                                .forEach { date ->
                                    submitScheduleMessage(
                                        workloadName,
                                        date,
                                        taskCallFactory
                                    )
                                }
                            delay(Settings.scheduleDelayDuration)
                        } catch (e: Exception) {
                            if (e is CancellationException) {
                                // catching CancellationException leads to dead loop
                                throw e
                            }
                            logger.error(e) { "Error in scheduler for $workloadName" }
                        }
                        delay(Settings.scheduleDelayDuration)
                    }
                }
            }
        }
    }

    private fun <T: Any> submitScheduleMessage(workloadName: String, scheduleAt: Instant, taskCallFactory: TaskCallFactory<T>) {
        val call = taskCallFactory(
            CallParams(
                callId = "${workloadName}-${scheduleAt}",
                delay = maxOf(scheduleAt - Clock.System.now(), Duration.ZERO)
            )
        )

        withLogCtx(
            "callId" to (call.message.headers["call-id"] ?: "unknown"),
            "attemptNum" to (call.message.headers["attempt-num"] ?: "unknown"),
            "taskName" to workloadName,
            "scheduleAt" to scheduleAt.toString()
        ) {
            if (!scheduler.recordScheduling(workloadName, scheduleAt)) {
                logger.trace { "Scheduling $workloadName. Record already exists." }
                return
            }

            logger.debug { "Scheduling $workloadName. Success." }

            enqueueTaskCall(call)
        }
    }

    fun startWorkers(vararg tasks: Task<*>) {
        if (broker is LocalBroker) {
            logger.warn { "LocalBroker is used. Workers are started automatically. Use startWorkers only for remote brokers" }
            return
        }
        tasks.forEach { startWorker(it) }

        // System worker
        startWorker(cleanScheduleWorker)
    }

    override fun close() {
        broker.close()
        scope.cancel()
    }
    init {
        setDefaultInstance(this)
    }

    companion object {
        val cleanScheduleWorkloadName = "kotask-system-schedule-clean"
        private var defaultInstance: TaskManager? = null
        fun setDefaultInstance(instance: TaskManager) {
            defaultInstance = instance
        }
        fun getDefaultInstance() = defaultInstance ?: throw Exception("Default instance is not initialized")
    }

    private fun checkTaskUniq(task: Task<*>) {
        // TODO(baitcode): Ask Ilya about this method. Seems redundant.
        knownTasks[task.name].let {
            if (it == null) {
                knownTasks[task.name] = task
            } else if (it != task) {
                throw Exception("Task with name ${task.name} already registered with different handler")
            }
        }
    }

    private fun messageToParams(msg: Message): CallParams {
        return CallParams(
            callId = msg.headers["call-id"] ?: "",
            attemptNum = msg.headers["attempt-num"]?.toInt() ?: 1,
            delay = msg.delayMs.toDuration(DurationUnit.MILLISECONDS),
        )
    }

    private fun paramsToMessage(inputStr: String, params: CallParams): Message {
        return Message(
            body = inputStr.encodeToByteArray(),
            headers = mapOf(
                "call-id" to params.callId,
                "attempt-num" to params.attemptNum.toString(),
            ),
            delayMs = params.delay.toLong(DurationUnit.MILLISECONDS),
        )
    }

    private fun queueNameByTaskName(taskName: String): QueueName = "${queueNamePrefix}${taskName}"

}

typealias QueueName = String

interface IMessageBroker: AutoCloseable {
    fun submitMessage(queueName: QueueName, message: Message)
    fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer
}

@Serializable
data class Message(
    val body: ByteArray,
    val headers: Map<String, String>,
    val delayMs: Long,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Message

        if (!body.contentEquals(other.body)) return false
        if (headers != other.headers) return false
        if (delayMs != other.delayMs) return false

        return true
    }

    override fun hashCode(): Int {
        var result = body.contentHashCode()
        result = 31 * result + headers.hashCode()
        result = 31 * result + delayMs.hashCode()
        return result
    }
}

interface IConsumer {
    fun stop()
}

typealias ConsumerHandler = suspend (message: Message, ack: ()->Any) -> Unit
