import com.zamna.kotask.LocalBroker
import com.zamna.kotask.TaskManager
import io.kotest.core.spec.style.FunSpec
import org.testcontainers.containers.PostgreSQLContainer
import plugins.scheduler.pg.PostgresqlScheduleTracker


class TaskManagerSchedulingPgTest : FunSpec({
    val pg = PostgreSQLContainer("postgres:14.1")
        .withDatabaseName("somedatabasename")
        .withUsername("postgres")
        .withPassword("postgres")
        .also { it.start() }


    include("Postgresql.", taskManagerSchedulingTest {
        TaskManager(
            LocalBroker(),
            scheduler = PostgresqlScheduleTracker(
                jdbcUrl = pg.jdbcUrl,
                user = pg.username,
                password = pg.password,
            )
        )
    })
})

