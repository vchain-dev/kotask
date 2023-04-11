
import kotlin.math.ceil
import kotlin.math.log2
import kotlin.math.pow

/**
 * Default exponential delay quantizer for RabbitMQ Backend.
 *
 * Converts delay value
 *
 *
 * @param `step` is the number of steps to divide the delay into. Bigger number means more steps.
 *
 * Positive delays less than 1s are quantized to 1s.
 * Quantized delays are always greater or equal to the original delay.
 *
 * For default `step` of 8, quants are the following (for all delays less than 24h):
 *
 * 1s
 * 2s
 * 3s
 * 4s
 * 5s
 * 6s
 * 7s
 * 8s
 * 10s
 * 12s
 * 14s
 * 16s
 * 20s
 * 24s
 * 28s
 * 32s
 * 40s
 * 48s
 * 56s
 * 1m 4s
 * 1m 20s
 * 1m 36s
 * 1m 52s
 * 2m 8s
 * 2m 40s
 * 3m 12s
 * 3m 44s
 * 4m 16s
 * 5m 20s
 * 6m 24s
 * 7m 28s
 * 8m 32s
 * 10m 40s
 * 12m 48s
 * 14m 56s
 * 17m 4s
 * 21m 20s
 * 25m 36s
 * 29m 52s
 * 34m 8s
 * 42m 40s
 * 51m 12s
 * 59m 44s
 * 1h 8m 16s
 * 1h 25m 20s
 * 1h 42m 24s
 * 1h 59m 28s
 * 2h 16m 32s
 * 2h 50m 40s
 * 3h 24m 48s
 * 3h 58m 56s
 * 4h 33m 4s
 * 5h 41m 20s
 * 6h 49m 36s
 * 7h 57m 52s
 * 9h 6m 8s
 * 11h 22m 40s
 * 13h 39m 12s
 * 15h 55m 44s
 * 18h 12m 16s
 * 22h 45m 20s
 * 1d 3h 18m 24s
 */
class ExpDelayQuantizer(private val step: Int = 8): IDelayQuantizer {

    override fun quantize(inputMs: Long): Long {
        val inputSec = ceil(inputMs.toDouble() / 1000)
        if (inputSec <= 0) {
            return 0
        }

        val powerOf2 = 2.0.pow(ceil(log2(inputSec)).toInt())/step
        val roundedSec = ceil(inputSec / powerOf2) * powerOf2
        return (roundedSec * 1000).toLong()
    }
}

interface IDelayQuantizer {
    fun quantize(inputMs: Long): Long
}
