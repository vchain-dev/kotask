
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class RetryPolicyTest : FunSpec({

    test("getRetryCallParams exp") {
        val policy = RetryPolicy(15.seconds, 20, expBackoff = true, maxDelay = 4.hours)
        var params = CallParams()

        listOf(
            15.seconds,
            30.seconds,
            1.minutes,
            2.minutes,
            4.minutes,
            8.minutes,
            16.minutes,
            32.minutes,
            1.hours + 4.minutes,
            2.hours + 8.minutes,
            4.hours,
            4.hours,
        ).forEach{ expectedDelay ->
            params = policy.getNextRetryCallParams(params).let{
                it.delay shouldBe expectedDelay
                it.attemptNum shouldBe params.attemptNum + 1
                it
            }
        }
    }
})
