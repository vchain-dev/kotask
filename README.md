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
val manager = TaskManager(RabbitMQBroker())
simpleTask.callLater("World", CallParams(delay = 3.seconds))
```

And deploy run worker process that will execute the job:
```kotlin
val manager = TaskManager(RabbitMQBroker())
manager.startWorkers(simpleTask)
```

## Usage
See [/example](src/main/kotlin/example) for a working example.

## RabbitMQ
RabbitMQ with [delayed messages plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) is required.
For local development, you can use [docker-compose.yml](docker-compose.yml) to start RabbitMQ with the plugin:
```bash
docker-compose up
```
It will start RabbitMQ on port 5672 and [management web interface](http://localhost:15672) on port 15672 with default credentials `guest:guest`.


## Notes

### Tasks

Currently framework supports 3 types of tasks.

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

Simplified task with context only. Does not have any input, but receives context by default.
```kotlin
val noArgTask = Task.create("no-arg-task") { 
    println("Hello, Stranger")
}
```
