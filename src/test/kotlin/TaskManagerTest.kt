import com.zamna.kotask.*
import io.kotest.assertions.throwables.shouldNotThrow
import io.kotest.common.ExperimentalKotest
import io.kotest.core.annotation.EnabledCondition
import io.kotest.core.spec.Spec
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import io.kotest.framework.concurrency.continually
import io.kotest.framework.concurrency.eventually
import io.kotest.framework.concurrency.until
import io.kotest.matchers.shouldBe
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import java.lang.Thread.sleep
import java.util.*
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration


fun env(name: String): String? = System.getenv().get(name)

class AzureConfigured : EnabledCondition {
    override fun enabled(kclass: KClass<out Spec>): Boolean = env("AZURE_URI") != null
}

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
    class UnhandledError : Exception()

    val errorHandler = mockk<TaskErrorHandler>()
    every { errorHandler.invoke(any(), any()) } returns Unit

    val serialisationErrorHandler = mockk<TaskErrorHandler>()
    every { serialisationErrorHandler.invoke(any(), any()) } returns Unit

    val tm = TaskManager(
        LocalBroker(),
        taskErrorHandlers = listOf(
            SerializationException::class.java to serialisationErrorHandler,
            UnhandledError::class.java to errorHandler
        )
    )

    val testTask1 =
        Task.create("failing-task-${randomSuffix()}",) { ctx, input: TaskTrackExecutionWithContextCountInput ->
            throw UnhandledError()
        }

    @Serializable
    data class TestTask2Input(
        val field1: String
    )

    val testTask2 = Task.create("failing-task-${randomSuffix()}") { ctx, input: TestTask2Input -> }

    beforeTest {
        clearMocks(errorHandler, serialisationErrorHandler)
    }

    test("test error") {
        tm.startWorkers(testTask1)

        TaskTrackExecutionWithContextCountInput.new().let {
            testTask1.callLater(it)
            eventually(1000) {
                verify( exactly = 0 ) { serialisationErrorHandler.invoke(any(), any()) }
                verify(exactly = 1) { errorHandler.invoke(any(), any()) }
            }
        }
    }

    test("test serialisation error") {
        tm.enqueueTaskCall(testTask2, "uadsfoianf", CallParams())

        tm.startWorkers(testTask2)

        eventually(1000) {
            verify( exactly = 0 ) { errorHandler.invoke(any(), any()) }
            verify( exactly = 1 ) { serialisationErrorHandler.invoke(any(), any()) }
        }
    }
})