package com.zamna.kotask

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch

class LocalBroker: IMessageBroker {
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
    }
}

class LocalQueue() {
    val channel = Channel<Message>(Channel.UNLIMITED)
}