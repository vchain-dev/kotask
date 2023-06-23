import com.zamna.kotask.Cron
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.time.withConstantNow
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockkObject
import kotlinx.datetime.*
import java.time.ZoneOffset
import kotlin.time.Duration.Companion.minutes


class SchedulePolicyTest: FunSpec({
    val start = LocalDateTime(2020, 1, 1, 0, 0, 0)
        .toInstant(TimeZone.UTC)
    val hourAfter = LocalDateTime(2020, 1, 1, 1, 0, 0)
        .toInstant(TimeZone.UTC)

    beforeSpec {
        mockkObject(Clock.System)
        every { Clock.System.now() } returns start
    }

    afterSpec {
        clearAllMocks()
    }

    test("Test that sequence of 60 records is populated for 1 hour period") {
        val everyMinute = Cron("* * * * *")
        val times = everyMinute.getNextCalls()
            .takeWhile { it < hourAfter }
            .toList()

        times.size shouldBe 60
        times.min() shouldBe start
        times.max() shouldBe hourAfter - 1.minutes
    }

})