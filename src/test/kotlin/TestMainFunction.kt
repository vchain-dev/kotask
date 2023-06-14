import com.zamna.kotask.*
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlin.time.Duration.Companion.seconds


@OptIn(DelicateCoroutinesApi::class)
fun main(vararg args: String) {
    val task = Task.create("testing-task1") {
        print("Executed")
    }
    val taskManager = TaskManager(
        LocalBroker()
    )

    Settings.scheduleDelayDuration = 1.seconds

    GlobalScope.launch {
        delay(100000)
        print("Stopping")
        taskManager.close()
    }

    taskManager.startWorkers(task)
    taskManager.startScheduler("task", Cron("* * * ? * * *"), task.prepareInput())
}