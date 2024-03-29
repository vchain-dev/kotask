package com.zamna.kotask

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
    override fun getNextRetryCallParams(params: CallParams): CallParams {
        val delay = if (expBackoff) {
             minOf(delay * 2.0.pow(params.attemptNum - 1), maxDelay)
        } else {
            delay
        }
        return params.nextAttempt().copy(delay = delay)
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

/**
 * An exception that indicates that task should be retried after delay.
 *
 *
 * @param delay Delay duration after which task should be retried.
 * @constructor Creates RetryControlException instance.
 */
class RepeatTask(val delay: Duration): RetryControlException() {
    fun getRetryCallParams(params: CallParams) = params.copy(
        delay = delay,
        attemptNum = 0
    )
}
class FailNoRetry(): RetryControlException()
