package com.zamna.kotask

import ExpDelayQuantizer
import IDelayQuantizer
import com.rabbitmq.client.*
import com.rabbitmq.client.impl.MicrometerMetricsCollector
import com.zamna.kotask.eventLogging.cDebug
import io.micrometer.core.instrument.logging.LoggingMeterRegistry
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

const val HEADERS_PREFIX = "kot-"

class RabbitMQBroker(
    uri: String = "amqp://guest:guest@localhost",
    metricsPrefix: String? = null,
    val delayQuantizer: IDelayQuantizer = ExpDelayQuantizer()
) : IMessageBroker {
    private val createdQueues = mutableSetOf<QueueName>()
    private val createdDelayQueues = mutableSetOf<Long>()
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

        // Create DELAYED exchange
        channel.exchangeDeclare(delayExchangeName, "headers", true)

    }

    // queue builder TODO:

    private fun assertQueue(queueName: QueueName) {
        if (queueName !in createdQueues) {
            logger.cDebug("Declare queue $queueName")
            val queueDeclare = channel.queueDeclare(queueName, true, false, false, null)
            logger.cDebug("Queue $queueName consumerCount=${queueDeclare.consumerCount} messageCount=${queueDeclare.messageCount}")
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
        // TODO: Log publish
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
            channel.queueDeclare(
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