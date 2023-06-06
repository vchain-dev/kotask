import com.zamna.kotask.*
import io.kotest.common.ExperimentalKotest
import io.kotest.core.extensions.install
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import io.kotest.extensions.testcontainers.TestContainerExtension
import io.kotest.framework.concurrency.continually
import io.kotest.framework.concurrency.eventually
import io.kotest.matchers.shouldBe
import org.testcontainers.containers.wait.strategy.Wait
import java.util.*
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration


class TaskManagerWithLocalBrokerTest: FunSpec({
    include("Local Broker", taskManagerTest(TaskManager(LocalBroker())))
})

class TaskManagerWithRabbitTest: FunSpec({
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

    afterSpec {
        taskManager.close()
    }

    include("Rabbit Broker", taskManagerTest(taskManager))
})

@OptIn(ExperimentalKotest::class)
fun taskManagerTest(taskManager: TaskManager) = funSpec {
    // Tasks to test
    val testTask1 = Task.create("testing-task1", ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
    }

    val testTask2 = Task.create("testing-task2", ) { input: TaskTrackExecutionInput ->
        input.markExecuted()
    }

    val testFailingTask = Task.create(
        "testing-failing-task",
        RetryPolicy(1.toDuration(DurationUnit.SECONDS), 3)
    ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }

    // Spec lifecycle

    beforeTest {
        TaskManager.setDefaultInstance(taskManager)
        taskManager.startWorkers(testTask1, testTask2, testFailingTask)
    }

    // Tests

    test("test basic queues, execution, delays") {

        TaskTrackExecutionWithContextCountInput.new().let {
            testTask1.callLater(it)
            eventually(1000) {
                it.isExecuted() shouldBe true
            }
        }

        TaskTrackExecutionInput.new().let {
            testTask2.callLater(it, CallParams(delay = 2.toDuration(DurationUnit.SECONDS)))
            continually(1500) {
                it.isExecuted() shouldBe false
            }
            eventually(1000) {
                it.isExecuted() shouldBe true
            }

        }
    }

    test("task with retries increment attempts and delay stays 1 on failure") {
        TaskTrackExecutionWithContextCountInput.new().let {
            testFailingTask.callLater(it)
            eventually(500) {
                it.executionsCount() shouldBe 1
            }
            val callId = it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 1
                it.delay shouldBe Duration.ZERO
                it.callId
            }
            eventually(1500) {
                it.executionsCount() shouldBe 2
            }
            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 2
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
            eventually(1500) {
                it.executionsCount() shouldBe 3
            }
            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 3
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
            eventually(1500) {
                it.executionsCount() shouldBe 3
            }
        }
    }

    test("TaskCall isExecuted updates after call") {
        TaskTrackExecutionWithContextCountInput.new().let {
            val call = testFailingTask.createTaskCall(it)
            continually(500) {
                it.isExecuted() shouldBe false
            }
            call.callLater()
            eventually(1000) {
                it.isExecuted() shouldBe true
            }
        }
    }
}
