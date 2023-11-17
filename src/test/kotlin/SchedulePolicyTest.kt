import com.zamna.kotask.Cron
import io.kotest.core.spec.style.FunSpec
import io.kotest.extensions.time.withConstantNow
import io.kotest.matchers.shouldBe
import io.mockk.clearAllMocks
import io.mockk.every
import io.mockk.mockkObject
import kotlinx.coroutines.delay
import kotlinx.datetime.*
import java.time.ZoneOffset
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds


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
            .takeWhile { it <= hourAfter }
            .toList()

        times.size shouldBe 60
        times.min() shouldBe start + 1.minutes
        times.max() shouldBe hourAfter
    }

    test("Test that sequence of 5 records is populated for 1 hour period") {
        val everyMinute = Cron("*/12 * * * *")
        val times = everyMinute.getNextCalls()
            .takeWhile { it < hourAfter }
            .toList()

        times.size shouldBe 4
    }

    test("Test that sequence of 5 equal records is populated for 1 hour period") {
        val everyMinute = Cron("*/12 * * * *")
        val times = everyMinute.getNextCalls()
            .takeWhile { it < hourAfter }
            .toList()

        times.size shouldBe 4

        delay(1.seconds)

        val everyMinute2 = Cron("*/12 * * * *")
        val times2 = everyMinute2.getNextCalls()
            .takeWhile { it < hourAfter }
            .toList()

        times2.size shouldBe 4

        times shouldBe times2


    }

})