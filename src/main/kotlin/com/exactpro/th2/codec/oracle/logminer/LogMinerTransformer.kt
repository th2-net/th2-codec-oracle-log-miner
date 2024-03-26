/*
 * Copyright 2023-2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.codec.oracle.logminer

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.UpdateListener
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.logId
import mu.KotlinLogging
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

class LogMinerTransformer(private val config: LogMinerConfiguration) : IPipelineCodec {
    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup =
        MessageGroup.builder().apply {
            messageGroup.messages.forEach { message ->
                if (!isAppropriate(message)) {
                    LOGGER.debug { "Skip message ${message.id.logId}" }
                    addMessage(message)
                    return@forEach
                }

                LOGGER.debug { "Begin process message ${message.id.logId}" }
                runCatching {
                    check(message.body.keys.containsAll(REQUIRED_COLUMNS)) {
                        error("Message doesn't contain required columns ${REQUIRED_COLUMNS.minus(message.body.keys)}, id: ${message.id.logId}")
                    }

                    val operation = requireNotNull(message.body[LOG_MINER_OPERATION_COLUMN]?.toString()) {
                        "Message doesn't contain required field '$LOG_MINER_OPERATION_COLUMN', id: ${message.id.logId}"
                    }
                    val sqlRedo = requireNotNull(message.body[LOG_MINER_SQL_REDO_COLUMN]?.toString()) {
                        "Message doesn't contain required field '$LOG_MINER_SQL_REDO_COLUMN', id: ${message.id.logId}"
                    }

                    when (operation) {
                        "INSERT" -> {
                            message.toBuilderWithoutBody().apply {
                                bodyBuilder().apply {
                                    InsertListener.parse(
                                        this,
                                        config.columnPrefix,
                                        config.trimParsedContent,
                                        sqlRedo
                                    )
                                }
                            }
                        }

                        "UPDATE" -> {
                            message.toBuilderWithoutBody().apply {
                                bodyBuilder().apply {
                                    UpdateListener.parse(
                                        this,
                                        config.columnPrefix,
                                        config.trimParsedContent,
                                        if (config.truncateUpdateQueryFromWhereClause) {
                                            truncateFromWhereClause(sqlRedo)
                                        } else {
                                            sqlRedo
                                        }
                                    )
                                }
                            }
                        }
                        "UNSUPPORTED" -> message.toBuilderWithoutBody()
                        "DELETE" -> message.toBuilderWithoutBody()
                        else -> error("Unsupported operation kind '$operation'")
                    }
                }.getOrElse { e ->
                    if (messageGroup.messages.size == 1) {
                        throw e
                    }

                    val error = e.extractMessage()

                    "Message transformation failure, id: ${message.id.logId}".also { text ->
                        LOGGER.error(e) { text }
                        context.warning("$text $error")
                    }

                    message.toBuilderWithoutBody().apply {
                        setType(ERROR_TYPE_MESSAGE)
                        bodyBuilder().put(ERROR_CONTENT_FIELD, error)
                    }
                }.apply {
                    setProtocol(TransformerFactory.AGGREGATED_PROTOCOL)
                    bodyBuilder().apply {
                        config.saveColumns.forEach { column ->
                            message.body[column]?.let { value -> put(column, value) }
                        }
                    }
                }.build().also(this::addMessage)

            }
        }.build()

    @OptIn(ExperimentalContracts::class)
    private fun isAppropriate(message: Message<*>): Boolean {
        contract {
            returns(true) implies (message is ParsedMessage)
        }

        return message is ParsedMessage
                && (message.protocol.isBlank() || TransformerFactory.PROTOCOLS.contains(message.protocol)).also {
                    if (!it) {
                        LOGGER.warn {
                            "The ${message.id.logId} message isn't appropriate, type: '${message::class.java}', protocol: '${message.protocol}'"
                        }
                    }
        }
    }

    companion object {
        val LOGGER = KotlinLogging.logger {}
        private const val LOG_MINER_OPERATION_COLUMN = "OPERATION"
        private const val LOG_MINER_SQL_REDO_COLUMN = "SQL_REDO"
        private const val LOG_MINER_ROW_ID_COLUMN = "ROW_ID"
        private const val LOG_MINER_TIMESTAMP_COLUMN = "TIMESTAMP"
        private const val LOG_MINER_TABLE_NAME_COLUMN = "TABLE_NAME"
        private const val WHERE_CLAUSE = "WHERE"

        internal val REQUIRED_COLUMNS: Set<String> = hashSetOf(
            LOG_MINER_OPERATION_COLUMN,
            LOG_MINER_SQL_REDO_COLUMN,
            LOG_MINER_ROW_ID_COLUMN,
            LOG_MINER_TIMESTAMP_COLUMN,
            LOG_MINER_TABLE_NAME_COLUMN,
        )

        internal fun truncateFromWhereClause(query: String): String {
            val whereIndex = query.indexOf(WHERE_CLAUSE, ignoreCase = true)
            return if (whereIndex == -1) {
                query
            } else {
                "${query.substring(0, whereIndex)};"
            }
        }

        internal fun ParsedMessage.toBuilderWithoutBody() = ParsedMessage.builder().apply {
            setId(this@toBuilderWithoutBody.id)
            this@toBuilderWithoutBody.eventId?.let(this::setEventId)
            setType(this@toBuilderWithoutBody.type)
            metadataBuilder().putAll(this@toBuilderWithoutBody.metadata)
            setProtocol(this@toBuilderWithoutBody.protocol)
        }

        private fun Throwable.extractMessage(): String? = message?.lines()?.first()
    }
}
