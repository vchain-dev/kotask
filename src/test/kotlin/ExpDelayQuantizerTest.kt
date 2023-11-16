import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import kotlin.time.Duration.Companion.hours
import kotlin.time.DurationUnit
import kotlin.time.toDuration

class ExpDelayQuantizerTest : FunSpec({
    val quantifier = ExpDelayQuantizer()
    test("ExpDelayQuantizerTest.quantize") {
        mapOf(
            -1 to 0,
            0 to 0,
            1 to 1000,
            30 to 1000,
            5000 to 5000,
            100000 to 112000,
        ).forEach() { (input, expectedOutput) ->
            quantifier.quantize(input.toLong()) shouldBe expectedOutput
        }
    }

    xtest("print all quants") {
        var prevQ: Long = 0
        for (i in 1..1000000000) {
            val quantified = quantifier.quantize(i.toLong())
            val duration = quantified.toDuration(DurationUnit.MILLISECONDS)
            if (prevQ != quantified) {
                println("* $duration")
            }
            prevQ = quantified
        }
    }

    test("Schedule message to 16 hours ahead") {
        val z = quantifier.quantize(16.hours.inWholeMilliseconds)
        print(z)
    }

})
