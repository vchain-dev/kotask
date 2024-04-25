import brokers.AzureServiceBusBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.annotation.EnabledIf
import io.kotest.core.spec.style.FunSpec

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