import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

object Settings {

    var scheduleDelayDuration: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_DELAY_SECONDS", "600")
        .toInt()
        .seconds

    var scheduleTableName: String = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_TABLE_NAME", "schedule") // 1 hour

    var messagesTableName: String = System.getenv()
        .getOrDefault("KOTASK_MESSAGES_TABLE_NAME", "kotask_messages") // 1 hour

    var scheduleTTL: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_CLEANUP_SECONDS", "3600") // 1 hour
        .toInt()
        .seconds

    var schedulingHorizon: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULING_HORIZON", "86400") // 1 day
        .toInt()
        .seconds

}