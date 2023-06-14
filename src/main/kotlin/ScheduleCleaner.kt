import com.zamna.kotask.Task
import com.zamna.kotask.TaskManager
import kotlinx.datetime.Clock

// TODO(baitcode): unclear how to test that
val cleanScheduleWorker = Task.create("kotask-system-schedule-clean-worker") {
    TaskManager.getDefaultInstance().scheduler.cleanScheduleOlderThan(
        Clock.System.now() - Settings.scheduleTTL
    )
}