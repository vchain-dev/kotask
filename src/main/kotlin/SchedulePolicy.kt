package com.zamna.kotask

import com.cronutils.model.CronType
import com.cronutils.model.definition.CronDefinitionBuilder
import com.cronutils.model.time.ExecutionTime
import com.cronutils.parser.CronParser
import kotlinx.datetime.*
import java.time.ZoneOffset

interface IRepeatingSchedulePolicy {
    fun getNextCalls(): Sequence<Instant>
}

private val parser: CronParser = CronParser(
    CronDefinitionBuilder.instanceDefinitionFor(CronType.UNIX)
)

class Cron(
    template: String,
    private val executionTime: ExecutionTime = ExecutionTime.forCron(parser.parse(template))
): IRepeatingSchedulePolicy {
    override fun getNextCalls(): Sequence<Instant> = generateSequence(
        Clock.System.now()
    ) {
        val javaInstant = it.toJavaInstant().atZone(ZoneOffset.systemDefault())
        val nexExecutionJavaZonedDt = executionTime.nextExecution(javaInstant).get()
        nexExecutionJavaZonedDt.toInstant().toKotlinInstant()
    }.drop(1) // drop seed
}


val onceAtMidnight = Cron("0 0 * * *")