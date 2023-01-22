import kotlin.time.Duration

interface IRetryPolicy {
    fun shouldRetry(params: CallParams): Boolean
    fun getRetryCallParams(params: CallParams): CallParams
}

data class RetryPolicy (
    val delay: Duration,
    val maxAttempts: Int
): IRetryPolicy {
    override fun shouldRetry(params: CallParams) = params.attemptNum <= maxAttempts
    override fun getRetryCallParams(params: CallParams) = params.copy(
        delay = delay,
        attemptNum = params.attemptNum + 1
    )
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

