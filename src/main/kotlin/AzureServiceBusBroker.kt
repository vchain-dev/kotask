import com.azure.messaging.servicebus.*
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder
import com.zamna.kotask.*
import kotlinx.coroutines.runBlocking
import java.time.OffsetDateTime
import java.util.*
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration
import kotlin.time.toJavaDuration

class AzureServiceBusBroker(
    val uri: String
): IMessageBroker {
    val delayHeaderName = "delay"

    private val manager = ServiceBusAdministrationClientBuilder()
        .connectionString(uri)
        .buildClient()

    private val queues: MutableMap<String, ServiceBusSenderClient> = mutableMapOf()

    private fun getSender(queueName: QueueName): ServiceBusSenderClient {
        return queues.getOrPut(queueName) {
            return ServiceBusClientBuilder()
                .connectionString(uri)
                .sender()
                .queueName(queueName)
                .buildClient()
        }
    }

    override fun submitMessage(queueName: QueueName, message: Message) {
        assertQueue(queueName)

        val msg = ServiceBusMessage(message.body)
            .setMessageId(UUID.randomUUID().toString())

        val kotaskHeaders = message.headers
            .mapKeys { (k, _) -> "$HEADERS_PREFIX$k" }
            .plus(delayHeaderName to message.delayMs.toString())

        msg.applicationProperties.putAll(kotaskHeaders)

        val delay = message.delayMs.toDuration(DurationUnit.MILLISECONDS)
        val enqueueTimeAtOffset = OffsetDateTime.now().plus(delay.toJavaDuration())

        if (delay > Duration.ZERO) {
            getSender(queueName)
                .scheduleMessage(msg, enqueueTimeAtOffset)
        } else {
            getSender(queueName)
                .sendMessage(msg)
        }
    }

    private fun assertQueue(queueName: QueueName) {
        if (manager.getQueueExists(queueName)) return

        manager.createQueue(queueName)
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        assertQueue(queueName)

        val receiver = ServiceBusClientBuilder()
            .connectionString(uri)
            .receiver()
            .queueName(queueName)
            .disableAutoComplete()
            .buildAsyncClient()

        receiver
            .receiveMessages()
            .subscribe { message ->
                // Context is unaccessible how to pass headers then?
                val allHeaders = message.applicationProperties

                val headers = allHeaders
                    .filterKeys { it.startsWith(HEADERS_PREFIX) }
                    .map { (k, v) -> k.toString().substring(HEADERS_PREFIX.length) to v.toString() }
                    .toMap()

                val msg = Message(
                    message.body.toBytes(),
                    headers = headers,
                    delayMs = allHeaders[delayHeaderName]?.toString()?.toLong() ?: 0,
                )

                runBlocking {
                    handler(msg) { }
                }
                receiver.complete(message).subscribe()
            }

        return AzureConsumer(receiver)
    }

    override fun close() {
        queues.values.forEach { it.close() }
    }

}

class AzureConsumer(private val consumer: ServiceBusReceiverAsyncClient) : IConsumer {
    override fun stop() {
        consumer.close()
    }
}