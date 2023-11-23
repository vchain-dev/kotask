import com.zamna.kotask.LocalBroker
import com.zamna.kotask.RabbitMQBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.testcontainers.TestContainerExtension
import org.testcontainers.containers.wait.strategy.Wait

class TaskManagerSchedulingRabbitTest: FunSpec({
    val rabbitUser = "guest"
    val rabbitPass = "guest"
    val rabbit = install(TestContainerExtension("rabbitmq:management")) {
        startupAttempts = 1
        withExposedPorts(5672)
        withEnv("RABBITMQ_DEFAULT_USER", rabbitUser)
        withEnv("RABBITMQ_DEFAULT_PASS", rabbitPass)
        waitingFor(Wait.forLogMessage(".*Server startup complete;.*\\n", 1));
    }

    val rabbitUri = "amqp://${rabbitUser}:${rabbitPass}@${rabbit.host}:${rabbit.firstMappedPort}"
    val taskManager = TaskManager(RabbitMQBroker(uri = rabbitUri))

    include("In memory.", taskManagerSchedulingTest(taskManager))
})