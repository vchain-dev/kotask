package brokers

import com.zamna.kotask.ConsumerHandler
import com.zamna.kotask.IConsumer
import com.zamna.kotask.Message
import com.zamna.kotask.QueueName

interface IMessageBroker: AutoCloseable {
    fun submitMessage(queueName: QueueName, message: Message)
    fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer
}