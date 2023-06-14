package com.zamna.kotask.scheduling

import com.zamna.kotask.LocalBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.spec.style.FunSpec
import io.kotest.framework.concurrency.eventually
import io.kotest.matchers.shouldBe
import org.testcontainers.containers.PostgreSQLContainer
import plugins.scheduler.pg.PostgresqlScheduleTracker
import java.util.*


class SchedulingPgTest: FunSpec({
    val pg = PostgreSQLContainer("postgres:14.1")
        .withDatabaseName("somedatabasename")
        .withUsername("postgres")
        .withPassword("postgres")
        .also{ it.start() }

    val taskManager = TaskManager(
        LocalBroker(),
        scheduler = PostgresqlScheduleTracker(
            jdbcUrl = pg.jdbcUrl,
            user = pg.username,
            password = pg.password,
        )
    )

    afterSpec {
        taskManager.close()
    }

    include("Postgresql.", schedulingTest(taskManager))
})


class SchedulingInMemoryTest: FunSpec({
    val taskManager = TaskManager(LocalBroker())

    include("In memory.", schedulingTest(taskManager))


})