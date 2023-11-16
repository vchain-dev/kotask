package com.zamna.kotask

import ExpDelayQuantizer
import IDelayQuantizer
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import com.zamna.kotask.eventLogging.cDebug
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import kotlin.time.Duration.Companion.minutes

const val HEADERS_PREFIX = "kot-"

class RabbitMQBroker(
    uri: String = "amqp://guest:guest@localhost",
    metricsPrefix: String? = null,
    val delayQuantizer: IDelayQuantizer = ExpDelayQuantizer(),
    schedulersScope: CoroutineScope = CoroutineScope(Dispatchers.Default)
) : IMessageBroker {
    private val createdQueues = mutableSetOf<QueueName>()
    private val createdDelayQueues = mutableSetOf<Long>()
    private var queues: RabbitMQQueues
    var connection: Connection
    var channel: Channel

    val delayExchangeName = "kotask-delayed-exchange"
    val defaultExchangeName = ""

    val delayQueueNamePrefix = "kotask-delayed-"
    val delayHeaderName = "delay"
    val delayQuantizedHeaderName = "delay-quantized"

    private var logger = LoggerFactory.getLogger(this::class.java)

    private var metricsCollector = MicrometerMetricsCollector(
        LoggingMeterRegistry { s -> logger.info(s) },
        metricsPrefix
    )

    init {
        val factory = ConnectionFactory()
        if (metricsPrefix != null) {
            factory.metricsCollector = metricsCollector
        }
        factory.setUri(uri)
        factory.isAutomaticRecoveryEnabled = true
        connection = factory.newConnection()
        channel = connection.createChannel()
        queues = RabbitMQQueues(channel)

        // Create DELAYED exchange
        channel.exchangeDeclare(delayExchangeName, "headers", true)

        schedulersScope.launch {
            // Queue metrics
            while (true) {
                for (d in queues.declarations) {
                    queues.declare(d)?.let {
                        logger.cDebug("Queue ${d.queueName}" +
                                " consumerCount=${it.consumerCount} " +
                                " messageCount=${it.messageCount}")
                    }
                }
                delay(5.minutes)
            }
        }
    }


    private fun assertQueue(queueName: QueueName) {
        if (queueName !in createdQueues) {
            logger.cDebug("Declare queue $queueName")
            queues.declare(queueName, true, false, false, null)
            createdQueues.add(queueName)
        }
    }

    override fun submitMessage(queueName: QueueName, message: Message) {
        assertQueue(queueName)

        val quantizedDelayMs = quantizeDelayAndAssertDelayedQueue(message.delayMs)
        val rabbitHeaders = message.headers
            .mapKeys { (k, _) -> "$HEADERS_PREFIX$k" }
            .plus(delayHeaderName to message.delayMs.toString())
            .plus(delayQuantizedHeaderName to quantizedDelayMs.toString())

        val props = AMQP.BasicProperties.Builder()
            .headers(rabbitHeaders)
            .build()

        val exchangeName = if (quantizedDelayMs > 0) {
            logger.cDebug(
                "Publish to $delayExchangeName" +
                        "  queue=$queueName" +
                        "  quantizedDelay=$quantizedDelayMs" +
                        "  delay=${props.headers[delayHeaderName]}"
            )
            delayExchangeName
        } else defaultExchangeName

        channel.basicPublish(exchangeName, queueName, props, message.body)
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        assertQueue(queueName)

        val cChannel = connection.createChannel()
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
                    delayMs = props.headers[delayHeaderName]?.toString()?.toLong() ?: 0,
                )

                runBlocking {
                    handler(msg) {
                        cChannel.basicAck(envelope.deliveryTag, false)
                    }
                }
            }
        }
        cChannel.basicConsume(queueName, false, c)
        return RabbitMQConsumer(c)
    }

    override fun close() {
        if (connection.isOpen) connection.close()
    }

    fun quantizeDelayAndAssertDelayedQueue(delayMs: Long): Long {
        val delayQuantized = delayQuantizer.quantize(delayMs)

        if (delayQuantized > 0 && delayQuantized !in createdDelayQueues) {
            logger.debug("Create delayed queue for $delayQuantized ms")
            val queueName = "$delayQueueNamePrefix$delayQuantized"
            queues.declare(
                queueName, true, false, false,
                mapOf("x-message-ttl" to delayQuantized, "x-dead-letter-exchange" to defaultExchangeName)
            )
            channel.queueBind(
                queueName, delayExchangeName, queueName,
                mapOf(delayQuantizedHeaderName to delayQuantized.toString(), "x-match" to "all")
            )
            createdDelayQueues.add(delayQuantized)
        }
        return delayQuantized
    }
}

class RabbitMQConsumer(val consumer: DefaultConsumer) : IConsumer {
    override fun stop() {
        consumer.channel.basicCancel(consumer.consumerTag)
    }
}


class RabbitMQQueues(
    var channel: Channel
) {
    data class RabbitMQQueueDeclaration(
        val queueName: String,
        val durable: Boolean,
        val exclusive: Boolean,
        val autoDelete: Boolean,
        val arguments: Map<String, Any>?
    )

    val declarations = mutableSetOf<RabbitMQQueueDeclaration>()

    fun declare(
        queueName: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: Map<String, Any>?
    ) {
        val declaration = RabbitMQQueueDeclaration(queueName, durable, exclusive, autoDelete, arguments)
        declarations.add(declaration)
        this.declare(declaration)
    }

    fun declare(d: RabbitMQQueueDeclaration): AMQP.Queue.DeclareOk? {
        return channel.queueDeclare(d.queueName, d.durable, d.exclusive, d.autoDelete, d.arguments)
    }
}