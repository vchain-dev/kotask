package com.zamna.kotask

import com.ucasoft.kcron.KCron
import com.ucasoft.kcron.builders.*
import com.ucasoft.kcron.common.WeekDays
import kotlinx.datetime.*

interface ISchedulePolicy {
    fun getNextCalls(): List<Instant>
}

class Cron(
    template: String,
    firstDayOfWeek: WeekDays = WeekDays.Monday,
    private val builder: Builder = KCron.parseAndBuild(template, firstDayOfWeek)
): ISchedulePolicy {
    // TODO: implement until
    override fun getNextCalls(): List<Instant> = builder
        .nextRunList()
        .map { it.toInstant(TimeZone.currentSystemDefault()) }
}
