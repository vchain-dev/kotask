import com.zamna.kotask.Cron
import io.kotest.core.spec.style.DescribeSpec
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.delay
import kotlinx.datetime.Clock
import kotlin.time.Duration.Companion.hours

class ScheduleGenerationWithoutMockedClockTrackerTest: DescribeSpec({

    describe("Two Schedules") {
        val everyMinute1 = Cron("* * * * *")
        delay(100)
        val everyMinute2 = Cron("* * * * *")

        it("Should generate same schedule regardless delay between them") {
            val hourAhead = Clock.System.now() + 1.hours

            val times1 = everyMinute1.getNextCalls()
                .takeWhile { it < hourAhead }
                .toList()

            delay(100)

            val times2 = everyMinute2.getNextCalls()
                .takeWhile { it < hourAhead }
                .toList()

            times1 shouldBe times2
        }
    }

})