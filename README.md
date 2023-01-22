# Kotask

## Usage
See [/example](src/main/kotlin/example) for a working example.

## RabbitMQ
RabbitMQ with [delayed messages plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) is required.
For local development, you can use [docker-compose.yml](docker-compose.yml) to start RabbitMQ with the plugin:
```bash
docker-compose up
```
It will start RabbitMQ on port 5672 and [management web interface](http://localhost:15672) on port 15672 with default credentials `guest:guest`.
