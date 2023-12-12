/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.logId
import mu.KotlinLogging
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.update.Update
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.contract

class LogMinerTransformer(private val config: LogMinerConfiguration) : IPipelineCodec {
    override fun decode(messageGroup: MessageGroup, context: IReportingContext): MessageGroup =
        MessageGroup.builder().apply {
            messageGroup.messages.forEach { message ->
                when (isAppropriate(message)) {
                    true -> {
                        LOGGER.debug { "Begin process message ${message.id.logId}" }
                        runCatching {
                            check(message.body.keys.containsAll(REQUIRED_COLUMNS)) {
                                error("Message doesn't contain required columns ${REQUIRED_COLUMNS.minus(message.body.keys)}")
                            }

                            val operation = requireNotNull(message.body[LOG_MINER_OPERATION_COLUMN]?.toString()) {
                                "Message doesn't contain required field '$LOG_MINER_OPERATION_COLUMN'"
                            }
                            val sqlRedo = requireNotNull(message.body[LOG_MINER_SQL_REDO_COLUMN]?.toString()) {
                                "Message doesn't contain required field '$LOG_MINER_SQL_REDO_COLUMN'"
                            }

                            when (operation) {
                                "INSERT" -> {
                                    val insert = CCJSqlParserUtil.parse(sqlRedo) as Insert
                                    check(insert.columns.size == insert.select.values.expressions.size) {
                                        "Incorrect query '$sqlRedo', column and value sizes mismatch"
                                    }
                                    message.toBuilderWithoutBody().apply {
                                        bodyBuilder().apply {
                                            insert.columns.forEachIndexed { index, column ->
                                                put(
                                                    "${config.columnPrefix}${column.columnName.trim('"')}",
                                                    insert.select.values.expressions[index].toReadable()
                                                )
                                            }
                                        }
                                    }
                                }

                                "UPDATE" -> {
                                    val update = CCJSqlParserUtil.parse(sqlRedo) as Update

                                    message.toBuilderWithoutBody().apply {
                                        bodyBuilder().apply {
                                            update.updateSets.forEach {
                                                put(
                                                    "${config.columnPrefix}${it.columns.single().columnName.trim('"')}",
                                                    it.values.single().toReadable()
                                                )
                                            }
                                        }
                                    }
                                }

                                "DELETE" -> message.toBuilderWithoutBody()
                                else -> error("Unsupported operation kind '$operation'")
                            }
                        }.getOrElse { e ->
                            LOGGER.error(e) { "Message transformation failure" }
                            message.toBuilderWithoutBody().apply {
                                setType(ERROR_TYPE_MESSAGE)
                                bodyBuilder().put(ERROR_CONTENT_FIELD, e.message)
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
                    false -> {
                        LOGGER.debug { "Skip message ${message.id.logId}" }
                        addMessage(message)
                    }
                }

            }
        }.build()

    @OptIn(ExperimentalContracts::class)
    private fun isAppropriate(message: Message<*>): Boolean {
        contract {
            returns(true) implies (message is ParsedMessage)
        }

        return message is ParsedMessage
                && (message.protocol.isBlank() || TransformerFactory.PROTOCOLS.contains(message.protocol)).also {
            LOGGER.debug { "The ${message.id.logId} message isn't appropriate, type: '${message::class.java}', protocol: '${message.protocol}'" }
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
        private const val LOG_MINER_OPERATION_COLUMN = "OPERATION"
        private const val LOG_MINER_SQL_REDO_COLUMN = "SQL_REDO"
        private const val LOG_MINER_ROW_ID_COLUMN = "ROW_ID"
        private const val LOG_MINER_TIMESTAMP_COLUMN = "TIMESTAMP"
        private const val LOG_MINER_TABLE_NAME_COLUMN = "TABLE_NAME"

        internal val REQUIRED_COLUMNS: Set<String> = hashSetOf(
            LOG_MINER_OPERATION_COLUMN,
            LOG_MINER_SQL_REDO_COLUMN,
            LOG_MINER_ROW_ID_COLUMN,
            LOG_MINER_TIMESTAMP_COLUMN,
            LOG_MINER_TABLE_NAME_COLUMN,
        )

        private fun ParsedMessage.toBuilderWithoutBody() = ParsedMessage.builder().apply {
            setId(this@toBuilderWithoutBody.id)
            this@toBuilderWithoutBody.eventId?.let(this::setEventId)
            setType(this@toBuilderWithoutBody.type)
            metadataBuilder().putAll(this@toBuilderWithoutBody.metadata)
            setProtocol(this@toBuilderWithoutBody.protocol)
        }

        private fun Expression.toReadable(): String {
            return when (this) {
                is Function -> parameters.single().toReadable()
                is StringValue -> value
                else -> error("Unsupported expression: $this, type: ${this::class.java}")
            }.trim()
        }
    }
}
