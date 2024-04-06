import brokers.KotaskMessages
import brokers.PgBroker
import com.zamna.kotask.RetryPolicy
import com.zamna.kotask.Task
import com.zamna.kotask.TaskManager
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import io.kotest.framework.concurrency.continually
import io.kotest.framework.concurrency.eventually
import io.kotest.matchers.shouldBe
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.testcontainers.containers.PostgreSQLContainer
import plugins.scheduler.pg.connectToDatabase
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class TaskManagerWithPGBrokerTest : FunSpec({

    val pg = PostgreSQLContainer("postgres:14.1")
        .withDatabaseName("kotask")
        .withUsername("postgres")
        .withPassword("postgres")
        .also { it.start() }


    include(
        "PG Broker", taskManagerTest(
            TaskManager(
                PgBroker(
                    connectToDatabase(pg.jdbcUrl, pg.username, pg.password),
                    emptyMessageDelay = 50.milliseconds,
                )
            )
        )
    )

    include(
        "PG Broker", additionalPostgresqlTests(
            TaskManager(
                PgBroker(
                    connectToDatabase(pg.jdbcUrl, pg.username, pg.password),
                    emptyMessageDelay = 50.milliseconds,
                )
            )
        )
    )


})

@OptIn(ExperimentalKotest::class)
fun additionalPostgresqlTests(taskManager: TaskManager) = funSpec {
    val testFailingTask = Task.create(
        "testing-failing-task-${randomSuffix()}",
        RetryPolicy(
            1000.toDuration(DurationUnit.MILLISECONDS),
            3,
            maxDelay = 1000.toDuration(DurationUnit.MILLISECONDS),
        )
    ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }

    val testFailingOnceTask = Task.create(
        "testing-failing-once-task-${randomSuffix()}",
        RetryPolicy(1000.toDuration(DurationUnit.MILLISECONDS), 0)
    ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }

    // Spec lifecycle

    beforeTest {
        transaction { KotaskMessages.deleteAll() }
        TaskManager.setDefaultInstance(taskManager)
        taskManager.startWorkers(testFailingTask, testFailingOnceTask)
    }

    test("test queue is being populated and cleaned on simple flow") {
        transaction { KotaskMessages.selectAll().count() shouldBe 0 }

        TaskTrackExecutionWithContextCountInput.new().let {
            val call = testFailingOnceTask.createTaskCall(it)
            continually(500) {
                it.isExecuted() shouldBe false
            }
            transaction { KotaskMessages.selectAll().count() shouldBe 0 }
            call.callLater()
            transaction { KotaskMessages.selectAll().count() shouldBe 1 }
            eventually(1100) {
                it.isExecuted() shouldBe true
            }
            transaction { KotaskMessages.selectAll().count() shouldBe 0 }
        }
    }

    test("Single failing task should only populated 1 row") {
        TaskTrackExecutionWithContextCountInput.new().let {
            transaction { KotaskMessages.selectAll().count() shouldBe 0 }
            testFailingTask.callLater(it)
            eventually(1100) {
                it.executionsCount() shouldBe 1
                transaction { KotaskMessages.selectAll().count() shouldBe 1 }
            }
            eventually(1100) {
                it.executionsCount() shouldBe 2
                transaction { KotaskMessages.selectAll().count() shouldBe 1 }
            }
            eventually(1100) {
                it.executionsCount() shouldBe 3
                transaction { KotaskMessages.selectAll().count() shouldBe 1 }
            }
            eventually(1100) {
                it.executionsCount() shouldBe 4
                transaction { KotaskMessages.selectAll().count() shouldBe 0 }
            }
        }
    }
}