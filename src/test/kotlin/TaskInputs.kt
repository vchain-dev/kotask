import com.zamna.kotask.ExecutionContext
import kotlinx.serialization.Serializable
import java.util.*

@Serializable
data class TaskTrackExecutionWithContextCountInput(val callId: String) {
    companion object {
        fun new(): TaskTrackExecutionWithContextCountInput {
            return TaskTrackExecutionWithContextCountInput(UUID.randomUUID().toString())
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
data class TaskTrackExecutionInput(val callId2: String) {
    companion object {
        fun new(): TaskTrackExecutionInput  {
            return TaskTrackExecutionInput(UUID.randomUUID().toString())
        }
        val executed: MutableSet<String> = mutableSetOf()
    }

    fun markExecuted() {
        executed.add(callId2)
    }

    fun isExecuted() = executed.contains(callId2)
}
