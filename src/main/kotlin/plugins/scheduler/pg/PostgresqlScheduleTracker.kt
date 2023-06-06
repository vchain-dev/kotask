package plugins.scheduler.pg

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

import com.zamna.kotask.IScheduleTracker
import kotlinx.datetime.Instant
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toJavaLocalDateTime
import kotlinx.datetime.toLocalDateTime
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.CurrentDateTime
import org.jetbrains.exposed.sql.javatime.datetime

import org.jetbrains.exposed.sql.transactions.transaction
import org.postgresql.util.PSQLException
import org.slf4j.LoggerFactory
import java.sql.SQLIntegrityConstraintViolationException

object Schedule : Table() {
    val workloadName: Column<String> = varchar("workload_id", 512)
    val scheduledAt = datetime("scheduled_at")
    val createdAt = datetime("createdAt").defaultExpression(CurrentDateTime)

    override val primaryKey = PrimaryKey(workloadName, scheduledAt, name = "PK")
}


class PostgresqlScheduleTracker(
    jdbcUrl: String,
    user: String? = null,
    password: String? = null,
    private val db: DbWrapper = connectToDatabase(jdbcUrl, user, password)
): IScheduleTracker {
    private val logger = LoggerFactory.getLogger(this::class.java)

    init {
        transaction(db.connection) {
            SchemaUtils.createMissingTablesAndColumns(Schedule)
        }
    }

    override fun recordScheduling(workloadName: String, scheduleAt: Instant): Boolean {
        try {
            transaction {
                Schedule.insert {
                    it[this.workloadName] = workloadName
                    it[this.scheduledAt] = scheduleAt.toLocalDateTime(TimeZone.UTC).toJavaLocalDateTime()
                }
            }
        } catch (e: ExposedSQLException) {
            if (e.isDuplicateKeyError()) return false
            throw e
        }
        return true
    }
}

fun ExposedSQLException.isDuplicateKeyError() = cause?.message?.contains("duplicate key value violates") ?: false

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
