import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

object Settings {

    var scheduleDelayDuration: Duration = System.getenv()
        .getOrDefault("KOTASK_SCHEDULE_DELAY_SECONDS", "60")
        .toInt()
        .seconds

}