package example

import com.zamna.kotask.*
import kotlinx.serialization.Serializable
import kotlin.time.Duration.Companion.seconds

val simpleTask = Task.create("simple-task") { input: String ->
    println("Hello, $input")
}

val retryOnceTask = Task.create("retry-once-task", RetryPolicy(30.seconds, 1)) { ctx, input: String ->
    println("attemptNum: ${ctx.callParams.attemptNum}")
    throw Exception("I'm $input")
}


class SomeService(private val dependency: String) {
    val serviceTask = Task.create("some-service-task") { input: TaskInput ->
        println("Task with dependency: $dependency")
    }

    @Serializable
    class TaskInput(val data: ByteArray)
}

fun main() {
    val manager = TaskManager(RabbitMQBroker())

    simpleTask.callLater("World", CallParams(delay = 3.seconds))
    retryOnceTask.callLater("Failure")

    val service = SomeService("dependency")
    service.serviceTask.callLater(SomeService.TaskInput(byteArrayOf(1, 2, 3)))

    manager.close()
}