
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit
import kotlin.time.toDuration


class TaskManager(
    private val broker: IMessageBroker,
    private val queueNamePrefix: String = "kotask-",
    val defaultRetryPolicy: IRetryPolicy = RetryPolicy(30.seconds, 3),
): AutoCloseable {
    private val knownTasks: MutableMap<String, Task<*>> = mutableMapOf()
    private var logger = LoggerFactory.getLogger(this::class.java)

    fun enqueueTaskCall(task: Task<*>, inputStr: String, params: CallParams) {
        logger.debug("Enqueue task ${task.name} with input $inputStr")
        checkTaskUniq(task)
        broker.submitMessage(queueName(task), paramsToMessage(inputStr, params))
    }

    fun startWorker(task: Task<*>) {
        logger.info("Starting worker for task ${task.name}")
        checkTaskUniq(task)
        broker.startConsumer(queueName(task)) { message: Message, ack: () -> Any ->
            task.execute(message.body.decodeToString(), messageToParams(message), this)
            ack()
        }
    }

    fun startWorkers(vararg tasks: Task<*>) {
        tasks.forEach { startWorker(it) }
    }

    override fun close() {
        broker.close()
    }

    init {
        setDefaultInstance(this)
    }

    companion object {
        private var defaultInstance: TaskManager? = null
        fun setDefaultInstance(instance: TaskManager) {
            defaultInstance = instance
        }
        fun getDefaultInstance() = defaultInstance ?: throw Exception("Default instance is not initialized")
    }

    private fun checkTaskUniq(task: Task<*>) {
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

    private fun queueName(task: Task<*>): QueueName = "${queueNamePrefix}${task.name}"

}

typealias QueueName = String

interface IMessageBroker: AutoCloseable {
    fun submitMessage(queueName: QueueName, message: Message)
    fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer
}

data class Message(
    val body: ByteArray,
    val headers: Map<String, String>,
    val delayMs: Long,
)

interface IConsumer {
    fun stop()
}

typealias ConsumerHandler = (message: Message, ack: ()->Any) -> Unit

