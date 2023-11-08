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
 * 2h 30m
 * 3h
 * 3h 30m
 * 4h
 * 4h 30m
 * 5h
 * 5h 30m
 * 6h
 * 6h 30m
 * 7h
 * 7h 30m
 * 8h
 * 8h 30m
 * 9h
 * 9h 30m
 * 10h
 * 10h 30m
 * 11h
 * 11h 30m
 * 12h
 * 12h 30m
 * 13h
 * 13h 30m
 * 14h
 * 14h 30m
 * 15h
 * 15h 30m
 * 16h
 * 16h 30m
 * 17h
 * 17h 30m
 * 18h
 * 18h 30m
 * 19h
 * 19h 30m
 * 20h
 * 20h 30m
 * 21h
 * 21h 30m
 * 22h
 * 22h 30m
 * 23h
 * 23h 30m
 * 1d
 * 1d 0h 30m
 * 1d 1h
 * 1d 1h 30m
 * 1d 2h
 * 1d 2h 30m
 * 1d 3h
 */
class ExpDelayQuantizer(
    private val step: Int = 8,
    private val linearFrom: Long = 7200000 /*2h*/,
    private val linearQuant: Long = 1800000 /*30min*/
) : IDelayQuantizer {

    override fun quantize(inputMs: Long): Long {
        val inputSec = ceil(inputMs.toDouble() / 1000)
        if (inputSec <= 0) {
            return 0
        } else if (inputMs > linearFrom) {
            val a = ceil(inputMs.toDouble()/linearQuant).toLong()
            val b = a * linearQuant
            return b
        }

        val powerOf2 = 2.0.pow(ceil(log2(inputSec)).toInt()) / step
        val roundedSec = ceil(inputSec / powerOf2) * powerOf2
        return (roundedSec * 1000).toLong()
    }
}

interface IDelayQuantizer {
    fun quantize(inputMs: Long): Long
}
