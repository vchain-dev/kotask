import com.zamna.kotask.NoInput
import com.zamna.kotask.Task
import kotlinx.datetime.Clock

// TODO(baitcode): unclear how to test that
val cleanScheduleWorker = Task.create("kotask-system-schedule-clean-worker") { ctx, _: NoInput ->
    ctx.taskManager.scheduler.cleanScheduleOlderThan(
        Clock.System.now() - Settings.scheduleTTL
    )
}
