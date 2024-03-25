package brokers

import MDCContext
import com.zamna.kotask.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.javatime.CurrentTimestamp
import org.jetbrains.exposed.sql.javatime.timestamp
import org.jetbrains.exposed.sql.transactions.transaction
import plugins.scheduler.pg.DbWrapper
import java.time.Instant
import java.util.UUID


object KotaskMessages : Table(name=Settings.messagesTableName) {
    val callId = uuid("call_id")
    val queueName: Column<String> = varchar("queue", 512)
    val body = binary("body")
    val headers = binary("headers")
    val delayMs = long("delay_ms").default(0)
    val scheduledAt = timestamp("scheduled_at").defaultExpression(CurrentTimestamp())
    val createdAt = timestamp("created_at").defaultExpression(CurrentTimestamp())
    val startedAt = timestamp("started_at").nullable().default(null)
    val completedAt = timestamp("completed_at").nullable().default(null)

    override val primaryKey = PrimaryKey(callId)

    init {
        index("message_selection", false, completedAt, queueName, scheduledAt, startedAt)
    }
}

private fun Message.getCallId() = UUID.fromString(this.headers["call-id"])

class PgBroker(
    val dbWrapper: DbWrapper,
    val scope: CoroutineScope
): IMessageBroker {
    override fun submitMessage(queueName: QueueName, message: Message): Unit = transaction(dbWrapper.connection) {
        KotaskMessages.insert {
            it[this.callId] = message.getCallId()
            it[this.queueName] = queueName
            it[this.body] = message.body
            it[this.delayMs] = message.delayMs
            it[this.scheduledAt] = Instant.now().plusMillis(message.delayMs)
            it[this.headers] = Json.encodeToString(message.headers).encodeToByteArray()
        }
    }

    override fun startConsumer(queueName: QueueName, handler: ConsumerHandler): IConsumer {
        var isStopped = false
        val job = scope.launch(MDCContext()) {
            while (!isStopped) {
                val messages = transaction {
                    val messages = KotaskMessages
                        .selectAll()
                        .andWhere {
                            (KotaskMessages.completedAt.isNull()) and
                            (KotaskMessages.queueName eq queueName) and
                            (KotaskMessages.scheduledAt less Instant.now()) and
                            (
                                KotaskMessages.startedAt less Instant.now().minusSeconds(5 * 60) or // settings
                                KotaskMessages.startedAt.isNull()
                            )
                        }
                        .limit(100) // settings
                        .forUpdate()
                        .map {
                            val headers = Json.decodeFromString<HashMap<String, String>>(it[KotaskMessages.headers].decodeToString())
                            Message(
                                body = it[KotaskMessages.body],
                                headers = headers,
                                delayMs = it[KotaskMessages.delayMs],
                            )
                        }

                    val callIds = messages.map { it.getCallId() }

                    KotaskMessages.update({
                        KotaskMessages.callId inList callIds
                    }) {
                        it[startedAt] = Instant.now()
                    }

                    messages
                }

                if (messages.isEmpty()) {
                    delay(5000) // TODO: move to settings
                    continue
                }

                messages.forEach { message ->
                    handler(message) {
                        transaction {
                            KotaskMessages.update({
                                KotaskMessages.callId eq message.getCallId()
                            }) {
                                it[completedAt] = Instant.now()
                            }
                        }
                    }
                }
            }
        }

        return object: IConsumer {
            override fun stop() {
                isStopped = true
                runBlocking {
                    // Timeout?
                    job.join()
                }
            }
        }
    }

    override fun close() {
        // Do nothing
    }

}