package example

import RabbitMQBroker
import TaskManager

fun main() {
    val manager = TaskManager(RabbitMQBroker())

    manager.startWorkers(
        simpleTask,
        retryOnceTask,
        SomeService("dependency").serviceTask
    )
}