import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

object Settings {

    var scheduleDelayDuration: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_DELAY_SECONDS", "600")
        .toInt()
        .seconds

    var scheduleTTL: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_CLEANUP_SECONDS", "3600") // 1 hour
        .toInt()
        .seconds

    var schedulingHorizon: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULING_HORIZON", "86400") // 1 day
        .toInt()
        .seconds
}