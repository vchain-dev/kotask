
import io.kotest.core.spec.style.FunSpec
import io.kotest.core.spec.style.funSpec
import io.kotest.framework.concurrency.continually
import io.kotest.framework.concurrency.eventually
import io.kotest.matchers.shouldBe
import kotlinx.serialization.Serializable
import java.util.*
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.toDuration


class TaskManagerTest: FunSpec({
    include("Rabbit Broker", taskManagerTest(RabbitMQBroker()))
    include("Local Broker", taskManagerTest(LocalBroker()))
})
fun taskManagerTest(broker: IMessageBroker) = funSpec{

    val taskManager = TaskManager(broker)

    val testTask = Task.create("testing-task", ) { ctx, input: TestingTaskInput ->
        input.markExecuted(ctx)
    }

    val testTask2 = Task.create("testing-task2", ) { input: TestingTaskInput2 ->
        input.markExecuted()
    }

    val testFailingTask = Task.create(
        "testing-failing-task",
        RetryPolicy(1.toDuration(DurationUnit.SECONDS), 3)
    ) { ctx, input: TestingTaskInput ->
        input.markExecuted(ctx)
        throw Exception("test exception")
    }
    beforeTest {
        TaskManager.setDefaultInstance(taskManager)
        if (broker !is LocalBroker) {
            taskManager.startWorkers(testTask, testTask2, testFailingTask)
        }
    }

    afterSpec {
        taskManager.close()
    }


    test("test basic queues, execution, delays") {

        TestingTaskInput.new().let {
            testTask.callLater(it)
            eventually(1000) {
                it.isExecuted() shouldBe true
            }
        }

        TestingTaskInput2.new().let {
            testTask2.callLater(it, CallParams(delay = 3.toDuration(DurationUnit.SECONDS)))
            continually(2500) {
                it.isExecuted() shouldBe false
            }
            eventually(1500) {
                it.isExecuted() shouldBe true
            }

        }
    }

    test("retries") {
        TestingTaskInput.new().let {
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
}

@Serializable
data class TestingTaskInput(val callId: String) {
    companion object {
        fun new(): TestingTaskInput  {
            return TestingTaskInput(UUID.randomUUID().toString())
        }
        val executed: MutableMap<String, MutableList<ExecutionContext>> = mutableMapOf()
    }

    fun markExecuted(ctx: ExecutionContext) {
        executed.getOrPut(callId) { mutableListOf() }.add(ctx)
    }

    fun isExecuted() = executed.containsKey(callId)
    fun executionsCount() = executed[callId]?.size ?: 0
    fun lastExecutionCtx() = executed[callId]?.lastOrNull()
}

@Serializable
data class TestingTaskInput2(val callId2: String) {
    companion object {
        fun new(): TestingTaskInput2  {
            return TestingTaskInput2(UUID.randomUUID().toString())
        }
        val executed: MutableSet<String> = mutableSetOf()
    }

    fun markExecuted() {
        executed.add(callId2)
    }

    fun isExecuted() = executed.contains(callId2)
}
