package com.zamna.kotask

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.datetime.*

interface IScheduleTracker {
    fun recordScheduling(workloadName: String, scheduleAt: Instant): Boolean

    fun cleanScheduleOlderThan(minimumScheduledAt: Instant)
}

class InMemoryScheduleTracker: IScheduleTracker {
    private val data: MutableMap<String, MutableSet<LocalDateTime>> = mutableMapOf()
    private val lock = Mutex(false)

    override fun recordScheduling(workloadName: String, scheduleAt: Instant): Boolean = runBlocking {
        lock.withLock(this) {
            val scheduledAtTimes = data.getOrDefault(workloadName, mutableSetOf())
            val wasScheduled = scheduledAtTimes.add(scheduleAt.toLocalDateTime(TimeZone.UTC))
            data[workloadName] = scheduledAtTimes
            wasScheduled
        }
    }

    override fun cleanScheduleOlderThan(minimumScheduledAt: Instant) = runBlocking {
        lock.withLock(this) {
            val localMinimumScheduledAt = minimumScheduledAt.toLocalDateTime(TimeZone.UTC)
            data.forEach { (_, dates) ->
                dates.removeIf { createdAt -> createdAt < localMinimumScheduledAt }
            }
        }
    }
}
