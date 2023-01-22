
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds


val DEFAULT_RETRY_POlICY = RetryPolicy(4.seconds, 20, expBackoff = true, maxDelay =  1.hours)

interface IRetryPolicy {
    fun shouldRetry(params: CallParams): Boolean
    fun getRetryCallParams(params: CallParams): CallParams
}

class RetryPolicy (
    val delay: Duration,
    val maxRetries: Int,
    val expBackoff: Boolean = false,
    val maxDelay: Duration = Duration.INFINITE
): IRetryPolicy {
    override fun shouldRetry(params: CallParams) = params.attemptNum <= maxRetries
    override fun getRetryCallParams(params: CallParams): CallParams {
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
    override fun getRetryCallParams(params: CallParams) = throw UnsupportedOperationException()
}

class ForeverRetryPolicy(
    val delay: Duration
): IRetryPolicy {
    override fun shouldRetry(params: CallParams) = true
    override fun getRetryCallParams(params: CallParams) = params.copy(
        delay = delay,
        attemptNum = params.attemptNum + 1
    )
}

class DefaultRetryPolicy: IRetryPolicy {
    override fun shouldRetry(params: CallParams) = throw UnsupportedOperationException()
    override fun getRetryCallParams(params: CallParams) = throw UnsupportedOperationException()
}


abstract class RetryControlException(): Exception()

class ForceRetry(val delay: Duration): RetryControlException() {
    fun getRetryCallParams(params: CallParams) = params.copy(
        delay = delay,
        attemptNum = params.attemptNum + 1
    )
}
class FailNoRetry(): RetryControlException()
