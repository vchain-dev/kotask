package example

import com.zamna.kotask.*

val scheduledTask1 = Task.create("scheduled-task") { input: String ->
    println("Hello, $input")
}

val scheduledTask2 = Task.create("scheduled-task") { input: Int ->
    println("Hello, $input")
}

val scheduledTask3 = Task.create("scheduled-task") {
    println("Hello, no input")
}

fun main() {
    val manager = TaskManager(RabbitMQBroker(), InMemoryScheduleTracker())

    manager.startWorkers(
        simpleTask,
        retryOnceTask,
        scheduledTask1, // NOTE: defined in 2 places
        scheduledTask2, // NOTE: defined in 2 places
        SomeService("dependency").serviceTask
    )
    manager.startScheduler("task1", Cron("* * * * *"), scheduledTask1.prepareInput("World"))
    manager.startScheduler("task2", Cron("* * * * *"), scheduledTask2.prepareInput(5))
    manager.startScheduler("task2", Cron("* * * * *"), scheduledTask3.prepareInput())
}