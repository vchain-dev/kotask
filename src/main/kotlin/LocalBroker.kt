package com.zamna.kotask

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlin.concurrent.thread

class LocalBroker: IMessageBroker {
    var active = true
    var localThread = thread {
        while (active) {
            Thread.sleep(200)
        }
    }
    var queues: MutableMap<QueueName, LocalQueue> = mutableMapOf()

    override fun submitMessage(queueName: QueueName, message: Message) {
        val q = queues.getOrPut(queueName) { LocalQueue() }

        GlobalScope.launch {
            delay(message.delayMs)
            q.channel.send(message)
        }
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        val q = queues.getOrPut(queueName) { LocalQueue() }

        val job = GlobalScope.launch {
            for (msg in q.channel) {
                handler(msg) {
                    // do nothing
                }
            }
        }
        return object: IConsumer {
            override fun stop() {
                job.cancel()
            }
        }
    }

    override fun close() {
        // do nothing
        active = false
    }
}

class LocalQueue() {
    val channel = Channel<Message>(Channel.UNLIMITED)
}