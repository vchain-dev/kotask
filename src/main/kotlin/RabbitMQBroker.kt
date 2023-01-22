import com.rabbitmq.client.*
import org.slf4j.LoggerFactory

class RabbitMQBroker(uri: String = "amqp://guest:guest@localhost"): IMessageBroker {
    private val queues = mutableMapOf<String, RabbitMQQueue>()
    var connection: Connection
    var channel: Channel
    val exchangeName = "task-delayed-exchange"

    private var logger = LoggerFactory.getLogger(this::class.java)

    init {
        val factory = ConnectionFactory()

        if (false && logger.isDebugEnabled) {
            factory.setTrafficListener(object : TrafficListener {
                override fun write(outboundCommand: Command?) {
                    logger.debug("RabbitMQ write: $outboundCommand")

                }

                override fun read(inboundCommand: Command?) {
                    logger.debug("RabbitMQ read: $inboundCommand")
                }
            })
        }

        factory.setUri(uri)
        connection = factory.newConnection()
        channel = connection.createChannel()
        channel.exchangeDeclare(
            exchangeName,
            "x-delayed-message",
            true,
            false,
            mapOf("x-delayed-type" to "direct")
        )

    }

    private fun getQueueById(queueName: QueueName): RabbitMQQueue {
        return queues.getOrPut(queueName) { RabbitMQQueue(queueName, this) }
    }

    override fun submitMessage(queueName: QueueName, message: Message) {
        getQueueById(queueName).submitMessage(message)
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        return getQueueById(queueName).startConsumer(handler)
    }

    override fun close() {
        connection.close()
    }
}
const val HEADERS_PREFIX = "kot-"


class RabbitMQQueue(val queueName: QueueName, val broker: RabbitMQBroker) {
    private val pubChannel: Channel
    init {
        //pubChannel = broker.connection.createChannel()
        pubChannel = broker.channel
        pubChannel.queueDeclare(queueName, true, false, false, null)
        pubChannel.queueBind(queueName, broker.exchangeName, queueName)
    }

    fun submitMessage(message: Message) {
        val rabbitHeaders = message.headers.map { (k, v) -> "$HEADERS_PREFIX$k" to v }.toMutableList().let {
            it.add("x-delay" to message.delayMs.toString())
            it.toMap()
        }

        val props = AMQP.BasicProperties.Builder()
            .headers(rabbitHeaders)
            .build()

        pubChannel.basicPublish(broker.exchangeName, queueName, props, message.body)
    }

    fun startConsumer(handler: ConsumerHandler): RabbitMQConsumer {
        val cChannel = broker.connection.createChannel()
        val c = object : DefaultConsumer(cChannel) {
            override fun handleDelivery(
                consumerTag: String,
                envelope: Envelope,
                props: AMQP.BasicProperties,
                body: ByteArray
            ) {
                val headers = props.headers
                    .filterKeys { it.startsWith(HEADERS_PREFIX) }
                    .map { (k, v) -> k.toString().substring(HEADERS_PREFIX.length) to v.toString() }
                    .toMap()

                val msg = Message(
                    body = body,
                    headers = headers,
                    delayMs = props.headers["x-delay"]?.toString()?.toLong() ?: 0,
                )

                handler(msg) {
                    cChannel.basicAck(envelope.deliveryTag, false)
                }
            }

        }

        cChannel.basicConsume(queueName, false, c)
        return RabbitMQConsumer(c)
    }
}

class RabbitMQConsumer(val consumer: DefaultConsumer): IConsumer {
    override fun stop() {
        consumer.channel.basicCancel(consumer.consumerTag)
    }
}