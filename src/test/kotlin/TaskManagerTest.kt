import com.zamna.kotask.*
import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.common.ExperimentalKotest
import io.kotest.core.annotation.EnabledCondition
import io.kotest.core.annotation.EnabledIf
import io.kotest.core.extensions.install
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import io.kotest.extensions.testcontainers.TestContainerExtension
import io.kotest.framework.concurrency.continually
import io.kotest.framework.concurrency.eventually
import io.kotest.framework.concurrency.until
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.launch
import kotlinx.datetime.Clock
import org.testcontainers.containers.wait.strategy.Wait
import java.lang.Thread.sleep
import java.util.*
import kotlin.reflect.KClass
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

    include("Rabbit Broker", taskManagerTest(taskManager))
})

fun env(name: String): String? = System.getenv().get(name)

class AzureConfigured : EnabledCondition {
    override fun enabled(kclass: KClass<out Spec>): Boolean = env("AZURE_URI") != null
}

@EnabledIf(AzureConfigured::class)
class TaskManagerWithAzureTest: FunSpec({
    val taskManager = TaskManager(
        AzureServiceBusBroker(env("AZURE_URI")!!)
    )

    afterSpec {
        taskManager.close()
    }

    include("Azure Broker", taskManagerTest(taskManager))
})

fun randomSuffix(): String = UUID.randomUUID().toString().substring(0, 10)

@OptIn(ExperimentalKotest::class)
fun taskManagerTest(taskManager: TaskManager) = funSpec {
    // Tasks to test
    val testTask1 = Task.create("testing-task1-${randomSuffix()}", ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
    }

    val testTask2 = Task.create("testing-task2-${randomSuffix()}", ) { input: TaskTrackExecutionInput ->
        input.markExecuted()
    }

    val testFailingTask = Task.create(
        "testing-failing-task-${randomSuffix()}",
        RetryPolicy(
            1.toDuration(DurationUnit.SECONDS),
            3,
            maxDelay = 1500.toDuration(DurationUnit.MILLISECONDS)
        )
    ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }

    val testFailingOnceTask = Task.create(
        "testing-failing-once-task-${randomSuffix()}",
        RetryPolicy(1.toDuration(DurationUnit.SECONDS), 0)
    ) { ctx, input: TaskTrackExecutionWithContextCountInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }

    // Spec lifecycle

    beforeTest {
        TaskManager.setDefaultInstance(taskManager)
        taskManager.startWorkers(testTask1, testTask2, testFailingTask, testFailingOnceTask)
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
            launch {
                testTask2.callLater(it, CallParams(delay = 2.toDuration(DurationUnit.SECONDS)))
            }
            it.isExecuted() shouldBe false
            continually(1500) {
                it.isExecuted() shouldBe false
            }
            eventually(1500) {
                it.isExecuted() shouldBe true
            }
        }



    }

    test("TaskCall isExecuted updates after call") {
        TaskTrackExecutionWithContextCountInput.new().let {
            val call = testFailingOnceTask.createTaskCall(it)
            continually(500) {
                it.isExecuted() shouldBe false
            }
            call.callLater()
            eventually(1000) {
                it.isExecuted() shouldBe true
            }
        }
    }

    test("task with retries increment attempts and delay stays 1 on failure!!!") {
        TaskTrackExecutionWithContextCountInput.new().let {
            testFailingTask.callLater(it)
            until(500) {
                it.executionsCount() == 1
            }
            val callId = it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 1
                it.delay shouldBe Duration.ZERO
                it.callId
            }
            // 1st retry
            until(3000) {
                it.executionsCount() == 2
            }
            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 2
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
            // 2nd retry
            until(3000) {
                it.executionsCount() == 3
            }
            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 3
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
            // 3rd retry
            until(3000) {
                it.executionsCount() == 4
            }
            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 4
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
            // Check there is no 4th retry
            sleep(3000)

            it.lastExecutionCtx()!!.callParams.let {
                it.attemptNum shouldBe 4
                it.delay shouldBe 1.toDuration(DurationUnit.SECONDS)
                it.callId shouldBe callId
            }
        }
    }

    test("should not fail if started twice") {

        shouldNotThrow<Exception> {
            TaskManager.setDefaultInstance(taskManager)
            taskManager.startWorkers(testTask1, testTask2, testFailingTask, testFailingOnceTask)
        }
    }

    test("TaskManager should have 5 consumers registered") {
        taskManager.tasksConsumers.keys shouldBe setOf(
            testTask1.name,
            testTask2.name,
            testFailingOnceTask.name,
            testFailingTask.name,
            cleanScheduleWorker.name,
        )
    }

}




class TaskManagerErrorHandling: FunSpec({
    class UnhadledError : Exception()

    val errorHandler = mockk<TaskErrorHandler>()
    every { errorHandler.invoke(any(), any()) } returns Unit

    val tm = TaskManager(
        LocalBroker(),
        taskErrorHandlers = listOf(
            UnhadledError::class.java to errorHandler
        )
    )

    val testTask1 =
        Task.create("failing-task-${randomSuffix()}",) { ctx, input: TaskTrackExecutionWithContextCountInput ->
            throw UnhadledError()
        }

    test("test error") {
        tm.startWorkers(testTask1)

        TaskTrackExecutionWithContextCountInput.new().let {
            testTask1.callLater(it)
            eventually(1000) {
                verify(exactly = 1) { errorHandler.invoke(any(), any()) }
            }

        }
    }
})