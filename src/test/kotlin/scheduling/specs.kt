package com.zamna.kotask.scheduling

import Settings
import TaskTrackExecutionWithContextCountInput
import cleanScheduleWorker
import com.zamna.kotask.ISchedulePolicy
import com.zamna.kotask.Task
import com.zamna.kotask.TaskManager
import io.kotest.common.ExperimentalKotest
import io.kotest.core.spec.style.funSpec
import io.kotest.framework.concurrency.eventually
import io.kotest.matchers.shouldBe
import kotlinx.datetime.Clock
import kotlinx.datetime.Instant
import org.slf4j.LoggerFactory
import java.util.UUID
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds


@OptIn(ExperimentalKotest::class)
fun schedulingTest(taskManager: TaskManager) = funSpec {
    Settings.scheduleDelayDuration = 1.seconds

    class ScheduleTestTaskPolicy(
        val count: Int = 10,
        val timeout: Int = 200
    ): ISchedulePolicy {
        val startingPoint = Clock.System.now()
        override fun getNextCalls(): List<Instant> = (0 until count).map {
            startingPoint + timeout.milliseconds * it
        }.toList()
    }

    val logger = LoggerFactory.getLogger(this::class.java)

    val schedulingNoInput = Task.create("testing-scheduling-noinput") {}

    val schedulingTask1 = Task.create("testing-scheduling-task1") { ctx, input: TaskTrackExecutionWithContextCountInput ->
        logger.info("Executed")
        input.markExecuted(ctx)
    }

    val schedulingTask2 = Task.create("testing-scheduling-task2") { ctx, input: TaskTrackExecutionWithContextCountInput ->
        logger.info("Executed")
        input.markExecuted(ctx)
    }

    test("Start schedulers. Check that schedule has no duplicates.") {
        TaskTrackExecutionWithContextCountInput.new().let {
            val schedule = ScheduleTestTaskPolicy()
            val uniqueWorkflowName = "task1_${UUID.randomUUID()}"
            taskManager.startScheduler(uniqueWorkflowName, schedule, schedulingTask1.prepareInput(it))
            eventually(6000) {
                it.isExecuted() shouldBe true
                it.executionsCount() shouldBe 10
            }
        }
    }

    test("Start scheduler. Test that only part of schedule was executed in 5 sec (once every sec).") {
        TaskTrackExecutionWithContextCountInput.new().let {
            val schedule = ScheduleTestTaskPolicy(timeout = 1000)
            val uniqueWorkflowName = "task1_${UUID.randomUUID()}"
            taskManager.startScheduler(uniqueWorkflowName, schedule, schedulingTask1.prepareInput(it))
            eventually(4900) {
                it.isExecuted() shouldBe true
                it.executionsCount() shouldBe 5
            }
        }
    }

    test("Start 2 schedulers simultaneously. Test that both complete") {
        val input1 = TaskTrackExecutionWithContextCountInput.new()
        val input2 = TaskTrackExecutionWithContextCountInput.new()
        val schedule = ScheduleTestTaskPolicy(timeout = 200)
        taskManager.startScheduler("task1_${UUID.randomUUID()}", schedule, schedulingTask1.prepareInput(input1))
        taskManager.startScheduler("task2_${UUID.randomUUID()}", schedule, schedulingTask2.prepareInput(input2))

        eventually(4000) {
            input1.isExecuted() shouldBe true
            input1.executionsCount() shouldBe 10
            input2.isExecuted() shouldBe true
            input2.executionsCount() shouldBe 10
        }
    }

    test("Start 2 schedulers simultaneously but with same name. Test that first one is only one to complete") {
        val uniqueWorkflowName = "task1_${UUID.randomUUID()}"
        TaskTrackExecutionWithContextCountInput.new().let {
            val schedule = ScheduleTestTaskPolicy(timeout = 1000)
            taskManager.startScheduler(uniqueWorkflowName, schedule, schedulingTask1.prepareInput(it))
            eventually(4900) {
                it.isExecuted() shouldBe true
                it.executionsCount() shouldBe 5
            }
        }
        TaskTrackExecutionWithContextCountInput.new().let {
            val schedule = ScheduleTestTaskPolicy(timeout = 1000)
            taskManager.startScheduler(uniqueWorkflowName, schedule, schedulingTask1.prepareInput(it))
            eventually(4900) {
                it.isExecuted() shouldBe false
                it.executionsCount() shouldBe 0
            }
        }

    }

    test("Start scheduler. Check that schedule cleaner also starts.") {
        TaskTrackExecutionWithContextCountInput.new().let {
            val schedule = ScheduleTestTaskPolicy()
            val uniqueWorkflowName = "task1_${UUID.randomUUID()}"
            taskManager.startScheduler(
                uniqueWorkflowName,
                schedule,
                schedulingNoInput.prepareInput()
            )
            eventually(500) {
                taskManager.knownSchedulerNames().contains(TaskManager.cleanScheduleWorkloadName)
                taskManager.knownWorkerNames().contains(cleanScheduleWorker.name)
            }
        }
    }


}
