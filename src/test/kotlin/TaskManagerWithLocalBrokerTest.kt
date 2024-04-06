import com.zamna.kotask.LocalBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.spec.style.FunSpec

class TaskManagerWithLocalBrokerTest: FunSpec({
    include("Local Broker", taskManagerTest(TaskManager(LocalBroker())))
})