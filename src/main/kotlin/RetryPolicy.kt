
import kotlin.math.pow
import kotlin.time.Duration


interface IRetryPolicy {
    fun shouldRetry(params: CallParams): Boolean
    fun getNextRetryCallParams(params: CallParams): CallParams
}

open class RetryPolicy (
    val delay: Duration,
    val maxRetries: Int,
    val expBackoff: Boolean = false,
    val maxDelay: Duration = Duration.INFINITE
): IRetryPolicy {
    override fun shouldRetry(params: CallParams) = params.attemptNum <= maxRetries
    override fun getNextRetryCallParams(params:  CallParams): CallParams {
        val delay = if (expBackoff) {
             minOf(delay * 2.0.pow(params.attemptNum - 1), maxDelay)
        } else {
            delay
        }
        return params.copy(delay = delay, attemptNum = params.attemptNum + 1)
    }
}

class NoRetryPolicy: IRetryPolicy {
    override fun shouldRetry(params: CallParams) = false
    override fun getNextRetryCallParams(params: CallParams) = throw UnsupportedOperationException()
}

class ForeverRetryPolicy(
    delay: Duration,
    expBackoff: Boolean = false,
    maxDelay: Duration = Duration.INFINITE
): RetryPolicy(delay, Int.MAX_VALUE, expBackoff, maxDelay), IRetryPolicy

abstract class RetryControlException(): Exception()

class ForceRetry(val delay: Duration): RetryControlException() {
    fun getRetryCallParams(params: CallParams) = params.copy(
        delay = delay,
        attemptNum = params.attemptNum + 1
    )
}
class FailNoRetry(): RetryControlException()
