package brokers

import MDCContext
import com.zamna.kotask.*
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.*
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.SerializationException
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.javatime.*
import org.jetbrains.exposed.sql.transactions.transaction
import org.jetbrains.exposed.sql.vendors.ForUpdateOption
import org.jetbrains.exposed.sql.vendors.PostgreSQLDialect
import org.jetbrains.exposed.sql.vendors.currentDialect
import plugins.scheduler.pg.DbWrapper
import java.time.Instant
import java.util.UUID
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes


object KotaskMessages : Table(name=Settings.messagesTableName) {
    val id = uuid("id").autoGenerate()
    val callId = uuid("call_id")
    val queueName: Column<String> = varchar("queue", 512)
    val body = binary("body")
    val headers = binary("headers")
    val delayMs = long("delay_ms").default(0)
    val attemptNum = integer("attempt_num").default(0)
    val scheduledAt = timestamp("scheduled_at").defaultExpression(CurrentTimestamp())
    val startedAt = timestamp("started_at").nullable().default(null)

    override val primaryKey = PrimaryKey(id)

    init {
        index("message_selection", false, queueName, scheduledAt, startedAt)
    }
}

private fun Message.getId() = UUID.fromString(this.headers[PgBroker.ID_HEADER])
private fun Message.getCallId() = UUID.fromString(this.headers["call-id"])
private fun Message.getAttemptNum() = Integer.parseInt(this.headers.getOrDefault("attempt-num", "0"))

class Interval(
    val duration: Duration,
) : Expression<Instant>() {
    override fun toQueryBuilder(queryBuilder: QueryBuilder) = queryBuilder {
        +when {
            (currentDialect as? PostgreSQLDialect) != null -> "INTERVAL '1 second' * ${duration.inWholeSeconds}"
            else -> TODO("not implemented")
        }
    }
}

class PgBroker(
    val dbWrapper: DbWrapper,
    val scope: CoroutineScope = GlobalScope,

    val emptyMessageDelay: Duration = 5000.milliseconds,
    val messageReservationTimeoutMs: Duration = 5.minutes,
): IMessageBroker {
    companion object {
        const val HEADERS_PREFIX = "kot-pg"
        const val ID_HEADER = "${HEADERS_PREFIX}-id"
    }

    val logger = KotlinLogging.logger {  }

    init {
        transaction(dbWrapper.connection) {
            SchemaUtils.createMissingTablesAndColumns(KotaskMessages)
        }
    }

    override fun submitMessage(queueName: QueueName, message: Message): Unit {

        transaction(dbWrapper.connection) {
            val id = UUID.randomUUID()
            KotaskMessages.insert {
                it[this.id] = id
                it[this.callId] = message.getCallId()
                it[this.queueName] = queueName
                it[this.body] = message.body
                it[this.attemptNum] = message.getAttemptNum()
                it[this.delayMs] = message.delayMs
                it[this.scheduledAt] = PlusOp(CurrentTimestamp(), Interval(message.delayMs.milliseconds), JavaInstantColumnType())
                it[this.headers] = Json.encodeToString(
                    buildMap {
                        putAll(message.headers)
                        put(ID_HEADER, id.toString())
                    }
                ).encodeToByteArray()
            }
        }
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        val job = scope.launch(MDCContext()) {
            while (isActive) {
                val messages = transaction(dbWrapper.connection) {
                    val messages = KotaskMessages
                        .select {
                            (KotaskMessages.queueName eq queueName) and
                            (KotaskMessages.scheduledAt less CurrentTimestamp()) and
                            (
                                KotaskMessages.startedAt less MinusOp(CurrentTimestamp(), Interval(messageReservationTimeoutMs), JavaInstantColumnType()) or
                                KotaskMessages.startedAt.isNull()
                            )
                        }
                        .limit(100) // settings
                        .forUpdate(ForUpdateOption.PostgreSQL.ForUpdate(ForUpdateOption.PostgreSQL.MODE.SKIP_LOCKED))
                        .map { message ->
                            try {
                                val headers =
                                    Json.decodeFromString<HashMap<String, String>>(message[KotaskMessages.headers].decodeToString())

                                Message(
                                    body = message[KotaskMessages.body],
                                    headers = headers,
                                    delayMs = message[KotaskMessages.delayMs],
                                )
                            } catch (e: SerializationException) {
                                logger.error(e) { "Message dropped due to SerializationException" }
                                KotaskMessages.deleteWhere { callId eq message[callId]}
                                null
                            }
                        }.filterNotNull()

                    val callIds = messages.map { it.getCallId() }

                    KotaskMessages.update({
                        KotaskMessages.callId inList callIds
                    }) {
                        it[startedAt] = CurrentTimestamp()
                    }

                    messages
                }

                if (messages.isEmpty()) {
                    delay(emptyMessageDelay)
                    continue
                }

                messages.forEach { message ->
                    scope.launch {
                        handler(message) {
                            transaction {
                                KotaskMessages.deleteWhere {
                                    id eq message.getId()
                                }
                            }
                        }
                    }
                }
            }
        }

        return object: IConsumer {
            override fun stop() {
                runBlocking {
                    // Timeout?
                    job.cancelAndJoin()
                }
            }
        }
    }

    override fun close() {
        // Do nothing
    }

}