package com.zamna.kotask

import ExpDelayQuantizer
import IDelayQuantizer
import MDCContext
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import kotlinx.coroutines.*
import loggingScope
import withLogCtx
import kotlin.time.Duration.Companion.minutes

const val HEADERS_PREFIX = "kot-"

// todo: need a better way to process headers

class RabbitMQBroker(
    uri: String = "amqp://guest:guest@localhost",
    metricsPrefix: String? = null,
    val delayQuantizer: IDelayQuantizer = ExpDelayQuantizer(),
    schedulersScope: CoroutineScope? = null,
) : IMessageBroker {
    private val createdQueues = mutableSetOf<QueueName>()
    private val createdDelayQueues = mutableSetOf<Long>()
    private var queues: QueueDefinitions
    var connection: Connection
    var channel: Channel

    val delayExchangeName = "kotask-delayed-exchange"
    val defaultExchangeName = ""

    val delayQueueNamePrefix = "kotask-delayed-"
    val delayHeaderName = "delay"
    val delayQuantizedHeaderName = "delay-quantized"

    private var logger = KotlinLogging.logger { }
    private val scope = schedulersScope ?: loggingScope(logger)

    private var metricsCollector = MicrometerMetricsCollector(
        LoggingMeterRegistry { s -> logger.info { s } },
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
        queues = QueueDefinitions(channel)

        // Create DELAYED exchange
        channel.exchangeDeclare(delayExchangeName, "headers", true)

        scope.launch(MDCContext()) {
            // Queue metrics
            while (true) {
                for (d in queues.declarations) {
                    queues.declare(d)?.let {
                        withLogCtx(
                            "queueName" to d.queueName,
                            "consumerCount" to it.consumerCount.toString(),
                            "messageCount" to it.messageCount.toString(),
                        ) {
                            logger.debug { "Queue ${d.queueName} metrics" }
                        }
                    }
                }
                delay(5.minutes)
            }
        }
    }


    private fun assertQueue(queueName: QueueName) {
        if (queueName !in createdQueues) {
            withLogCtx("queueName" to queueName) {
                logger.debug { "Declare queue $queueName" }
                queues.declare(queueName, true, false, false, null)
                createdQueues.add(queueName)
            }
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

        withLogCtx(
            "callId" to (message.headers["call-id"] ?: "unknown"),
            "queueName" to queueName,
            "quantizedDelayMs" to quantizedDelayMs.toString(),
            "delay" to (props.headers[delayHeaderName]?.toString() ?: "unknown")
        ) {
            logger.debug { "Publish to delay exchange" }

            val exchangeName = if (quantizedDelayMs > 0) {
                delayExchangeName
            } else defaultExchangeName

            synchronized(channel) {
                channel.basicPublish(exchangeName, queueName, props, message.body)
            }
        }
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        assertQueue(queueName)

        val cChannel = connection.createChannel()
        val c = object : DefaultConsumer(cChannel) {

            override fun handleDelivery(
                consumerTag: String,
                envelope: Envelope,
                props: AMQP.BasicProperties,
                body: ByteArray,
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

                runBlocking(MDCContext()) {
                    handler(msg) {
                        synchronized(cChannel) {
                            cChannel.basicAck(envelope.deliveryTag, false)
                        }
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
        val queueName = "$delayQueueNamePrefix$delayQuantized"
        withLogCtx(
            "queueName" to queueName,
            "delayQuantized" to delayQuantized.toString(),
        ) {
            if (delayQuantized > 0 && delayQuantized !in createdDelayQueues) {
                logger.debug { "Create delayed queue" }
                queues.declare(
                    queueName,
                    durable = true,
                    exclusive = false,
                    autoDelete = false,
                    arguments = mapOf(
                        "x-message-ttl" to delayQuantized,
                        "x-dead-letter-exchange" to defaultExchangeName
                    )
                )
                channel.queueBind(
                    queueName, delayExchangeName, queueName,
                    mapOf(delayQuantizedHeaderName to delayQuantized.toString(), "x-match" to "all")
                )
                createdDelayQueues.add(delayQuantized)
            }
        }
        return delayQuantized
    }
}

class RabbitMQConsumer(val consumer: DefaultConsumer) : IConsumer {
    override fun stop() {
        consumer.channel.basicCancel(consumer.consumerTag)
    }
}


private class QueueDefinitions(
    var channel: Channel,
) {
    data class QueueDeclaration(
        val queueName: String,
        val durable: Boolean,
        val exclusive: Boolean,
        val autoDelete: Boolean,
        val arguments: Map<String, Any>?,
    )

    val declarations = mutableSetOf<QueueDeclaration>()

    fun declare(
        queueName: String,
        durable: Boolean,
        exclusive: Boolean,
        autoDelete: Boolean,
        arguments: Map<String, Any>?,
    ) {
        val declaration =
            QueueDeclaration(queueName, durable, exclusive, autoDelete, arguments)
        declarations.add(declaration)
        this.declare(declaration)
    }

    fun declare(d: QueueDeclaration): AMQP.Queue.DeclareOk? {
        return channel.queueDeclare(d.queueName, d.durable, d.exclusive, d.autoDelete, d.arguments)
    }
}
