import brokers.RabbitMQBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.testcontainers.TestContainerExtension
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.containers.wait.strategy.Wait
import plugins.scheduler.pg.PostgresqlScheduleTracker

class TaskManagerSchedulingRabbitTest : FunSpec({
    val rabbitUser = "guest"
    val rabbitPass = "guest"
    val rabbit = install(TestContainerExtension("rabbitmq:management")) {
        startupAttempts = 1
        withExposedPorts(5672)
        withEnv("RABBITMQ_DEFAULT_USER", rabbitUser)
        withEnv("RABBITMQ_DEFAULT_PASS", rabbitPass)
        waitingFor(Wait.forLogMessage(".*Server startup complete;.*\\n", 1));
    }
    val pg = PostgreSQLContainer("postgres:14.1")
        .withDatabaseName("somedatabasename")
        .withUsername("postgres")
        .withPassword("postgres")
        .also { it.start() }

    val rabbitUri = "amqp://${rabbitUser}:${rabbitPass}@${rabbit.host}:${rabbit.firstMappedPort}"
    include("Rabbit.", taskManagerSchedulingTest {
        TaskManager(
            RabbitMQBroker(uri = rabbitUri),
            scheduler = PostgresqlScheduleTracker(
                jdbcUrl = pg.jdbcUrl,
                user = pg.username,
                password = pg.password,
            )
        )
    })
})
