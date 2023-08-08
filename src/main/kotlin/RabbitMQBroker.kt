package com.zamna.kotask

import ExpDelayQuantizer
import IDelayQuantizer
import com.rabbitmq.client.*
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory

const val HEADERS_PREFIX = "kot-"

class RabbitMQBroker(
    uri: String = "amqp://guest:guest@localhost",
    val delayQuantizer: IDelayQuantizer = ExpDelayQuantizer()
) : IMessageBroker {
    private val createdQueues = mutableSetOf<QueueName>()
    private val createdDelayQueues = mutableSetOf<Long>()
    var connection: Connection
    var channel: Channel

    val delayExchangeName = "kotask-delayed-exchange"
    val delayQueueNamePrefix = "kotask-delayed-"
    val delayHeaderName = "delay"
    val delayQuantizedHeaderName = "delay-quantized"

    private var logger = LoggerFactory.getLogger(this::class.java)

    init {
        val factory = ConnectionFactory()
        factory.setUri(uri)
        connection = factory.newConnection()
        channel = connection.createChannel()

        // Create DELAYED exchange
        channel.exchangeDeclare(delayExchangeName, "headers", true)
    }

    private fun assertQueue(queueName: QueueName)  {
        if (queueName !in createdQueues) {
            logger.debug("Declare queue $queueName")
            channel.queueDeclare(queueName, true, false, false, null)
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

        val exchangeName = if (quantizedDelayMs > 0) delayExchangeName else ""
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
                mapOf("x-message-ttl" to delayQuantized, "x-dead-letter-exchange" to "")
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