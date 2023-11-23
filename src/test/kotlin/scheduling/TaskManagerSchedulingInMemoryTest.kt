import com.zamna.kotask.LocalBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.spec.style.FunSpec

class TaskManagerSchedulingInMemoryTest : FunSpec({

    include("In memory.", taskManagerSchedulingTest { TaskManager(LocalBroker()) })

})

