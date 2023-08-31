# Kotask

Framework for describing asynchronous background Jobs. We try to replicate functionality
of Python Celery framework. The idea is to build tool that will allow to write simple Task
definitions, such as:

```kotlin
val simpleTask = Task.create("simple-task") { input: String ->
    println("Hello, $input")
}
```

And then schedule job execution:
```kotlin
val manager = TaskManager(RabbitMQBroker()) // This is one of supported brokers
simpleTask.callLater("World", CallParams(delay = 3.seconds)) // Will execute job in 3 seconds from now
```

And deploy run worker process that will execute the job:
```kotlin
val manager = TaskManager(RabbitMQBroker())
manager.startWorkers(simpleTask)
```

## Usage
See [/example](src/main/kotlin/example) for examples.

## Notes

### Tasks

Currently, framework supports 2 types of tasks.

Normal task. This type of task allows you to access taskManager through context properties.
As well as additional metadata.
```kotlin
val task = Task.create("task") { ctx: ExecutionContext, input: String ->
    println("Hello, $input")
}
```

Simplified task with input only. Should be used for task, that don't populate new tasks.
```kotlin
val simpleTask = Task.create("simple-task") { input: String ->
    println("Hello, $input")
}
```

### Brokers

Currently, supported brokers are:

- RabbitMQ (GCP, AWS)
- Azure Service Bus

