package plugins.scheduler.pg

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import com.zamna.kotask.IScheduleTracker
import kotlinx.datetime.*
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.lessEq
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.CurrentTimestamp
import org.jetbrains.exposed.sql.javatime.datetime
import org.jetbrains.exposed.sql.javatime.timestamp

import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory

object Schedule : Table() {
    val workloadName: Column<String> = varchar("workload_id", 512)
    val scheduledAt = timestamp("scheduled_at")
    val createdAt = timestamp("createdAt").defaultExpression(CurrentTimestamp())

    override val primaryKey = PrimaryKey(workloadName, scheduledAt, name = "PK")
}

class PostgresqlScheduleTracker(
    jdbcUrl: String,
    user: String? = null,
    password: String? = null,
    private val db: DbWrapper = connectToDatabase(jdbcUrl, user, password)
): IScheduleTracker {

    init {
        transaction(db.connection) {
            SchemaUtils.createMissingTablesAndColumns(Schedule)
        }
    }

    override fun recordScheduling(workloadName: String, scheduleAt: Instant): Boolean {
        val res = transaction(db.connection) {
            Schedule.insertIgnore {
                it[this.workloadName] = workloadName
                it[this.scheduledAt] = scheduleAt.toJavaInstant()
            }
        }
        return res.insertedCount == 1
    }

    override fun cleanScheduleOlderThan(minimumScheduledAt: Instant) {
        transaction(db.connection) {
            Schedule.deleteWhere {
                this.scheduledAt.lessEq(minimumScheduledAt.toJavaInstant())
            }
        }
    }
}

data class DbWrapper(
    val connection: Database,
    val dataSource: HikariDataSource
)

var dbCache: MutableMap<String, DbWrapper> = mutableMapOf()

fun connectToDatabase(
    jdbcUrl: String,
    user: String? = null,
    password: String? = null,
    driverClassName: String = "org.postgresql.Driver"
): DbWrapper {
    if (dbCache.containsKey(jdbcUrl)) {
        return dbCache[jdbcUrl]!!
    }

    val config = HikariConfig()
    config.jdbcUrl = jdbcUrl
    config.initializationFailTimeout = 1000L * 60 * 2

    if (user != null) {
        config.username = user
        config.password = password
    }
    config.driverClassName = driverClassName

    val dataSource = HikariDataSource(config)
    val db = Database.connect(
        dataSource,
        databaseConfig = DatabaseConfig {
            defaultRepetitionAttempts = 0
        }
    )
    transaction(db) {
        addLogger(Slf4jSqlDebugLogger)
    }
    val res = DbWrapper(
        db,
        dataSource
    )
    dbCache[jdbcUrl] = res
    return res
}
