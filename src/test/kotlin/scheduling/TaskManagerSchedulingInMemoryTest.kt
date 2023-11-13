import com.zamna.kotask.LocalBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.spec.style.FunSpec

class TaskManagerSchedulingInMemoryTest: FunSpec({
    val taskManager = TaskManager(LocalBroker())

    include("In memory.", taskManagerSchedulingTest(taskManager))
})

