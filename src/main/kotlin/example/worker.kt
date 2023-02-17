package example

import com.zamna.kotask.RabbitMQBroker
import com.zamna.kotask.TaskManager

fun main() {
    val manager = TaskManager(RabbitMQBroker())

    manager.startWorkers(
        simpleTask,
        retryOnceTask,
        SomeService("dependency").serviceTask
    )
}