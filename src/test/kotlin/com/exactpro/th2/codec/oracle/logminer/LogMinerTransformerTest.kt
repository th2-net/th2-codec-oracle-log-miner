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

import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.oracle.logminer.LogMinerTransformer.Companion.toReadable
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.logId
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import net.sf.jsqlparser.JSQLParserException
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import strikt.api.expectThat
import strikt.assertions.filterIsInstance
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNull
import strikt.assertions.single
import strikt.assertions.withElementAt
import java.time.Instant

class LogMinerTransformerTest {
    private val reportingContext: IReportingContext = mock { }

    @AfterEach
    fun afterEach() {
        verifyNoMoreInteractions(reportingContext)
    }

    @Test
    fun decodesDataUsingDefaultHeader() {
        val config = LogMinerConfiguration()
        val codec = LogMinerTransformer(config)
        val sourceMessages: List<ParsedMessage> = loadMessages()
        assertEquals(4, sourceMessages.size)

        sourceMessages[0].let { source ->
            assertEquals(67, source.body.size)
            val message = assertThrows<IllegalStateException> {
                codec.decode(source.toGroup(), reportingContext)
            }.message
            assertEquals("Unsupported operation kind 'DDL'", message)
        }
        sourceMessages[1].let { source ->
            assertEquals(67, source.body.size)
            expectThat(codec.decode(source.toGroup(), reportingContext).messages).hasSize(1)
                .filterIsInstance<ParsedMessage>().apply {
                    hasSize(1)
                    single().apply {
                        get { id }.isEqualTo(source.id)
                        get { eventId }.isEqualTo(source.eventId)
                        get { type }.isEqualTo("test-type")
                        get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                        get { body }.isEqualTo(mapOf(
                            "th2_SALARY" to "110,000",
                            "th2_DENTAL_PLAN" to "Delta Dental",
                            "th2_ID" to "1",
                            "th2_SICK_TIME" to "5",
                            "th2_PLAN" to "25000",
                            "th2_TITLE" to "Manager",
                            "th2_TIME_OFF" to "15",
                            "th2_HEALTH_PLAN" to "Blue Cross and Blue Shield",
                            "th2_NAME" to "Chris Montgomery",
                            "th2_VISION_PLAN" to "Aetna Vision",
                            "th2_SAVINGS" to "1",
                            "th2_BONUS_STRUCTURE" to "5% Quarterly",
                        ) + source.body.filterKeys { config.saveColumns.contains(it) })
                    }
                }
        }
        sourceMessages[2].let { source ->
            assertEquals(67, source.body.size)
            expectThat(codec.decode(source.toGroup(), reportingContext).messages).hasSize(1)
                .filterIsInstance<ParsedMessage>().apply {
                    hasSize(1)
                    single().apply {
                        get { id }.isEqualTo(source.id)
                        get { eventId }.isEqualTo(source.eventId)
                        get { type }.isEqualTo("test-type")
                        get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                        get { body }.isEqualTo(mapOf(
                            "th2_SAVINGS" to "10"
                        ) + source.body.filterKeys { config.saveColumns.contains(it) })
                    }
                }
        }
    }

    @Test
    fun `decode broken row messages in one group test`() {
        val config = LogMinerConfiguration()
        val codec = LogMinerTransformer(config)
        val baseId = MessageId.builder()
            .setSessionAlias("test-session-alias")
            .setDirection(Direction.OUTGOING)
            .setTimestamp(Instant.now())
            .setSequence(1)
            .build()
        val source: List<ParsedMessage> = listOf(
            ParsedMessage.builder()
                .setId(baseId.copy(subsequence = listOf(1)))
                .setBody(
                    mapOf(
                        "OPERATION" to "INSERT",
                        "SQL_REDO" to "broken query",
                        "ROW_ID" to 1,
                        "TIMESTAMP" to Instant.now().toString(),
                        "TABLE_NAME" to "test-table",
                    )
                )
                .setType("test-message")
                .setProtocol("")
                .build(),
            ParsedMessage.builder()
                .setId(baseId.copy(subsequence = listOf(2)))
                .setBody(
                    mapOf(
                        "OPERATION" to "broken operation",
                        "SQL_REDO" to """insert into "OWNER"."table"("NAME","TIMESTAMP","DATE","NUMBER","NULL") 
                                        values ('An',TO_TIMESTAMP('12-DEC-23 02.55.01 PM'), TO_DATE('12-DEC-23', 'DD-MON-RR'), 8, NULL);""",
                        "ROW_ID" to 2,
                        "TIMESTAMP" to Instant.now().toString(),
                        "TABLE_NAME" to "test-table",
                    )
                )
                .setType("test-message")
                .setProtocol("")
                .build(),
        )

        expectThat(
            codec.decode(
                MessageGroup.builder().apply { source.forEach(::addMessage) }.build(),
                reportingContext
            ).messages
        ).hasSize(2)
            .filterIsInstance<ParsedMessage>().and {
                hasSize(2)
                withElementAt(0) {
                    val message = source[0]
                    get { id }.isEqualTo(message.id)
                    get { eventId }.isEqualTo(message.eventId)
                    get { type }.isEqualTo("th2-codec-error")
                    get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                    get { body }.isEqualTo(
                        mapOf(
                            "content" to """Encountered unexpected token: "broken" <S_IDENTIFIER>"""
                        ) + message.body.filterKeys { config.saveColumns.contains(it) }
                    )
                }
                withElementAt(1) {
                    val message = source[1]
                    get { id }.isEqualTo(message.id)
                    get { eventId }.isEqualTo(message.eventId)
                    get { type }.isEqualTo("th2-codec-error")
                    get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                    get { body }.isEqualTo(
                        mapOf(
                            "content" to "Unsupported operation kind 'broken operation'"
                        ) + message.body.filterKeys { config.saveColumns.contains(it) }
                    )
                }
            }
        verify(reportingContext).warning(eq("""Message transformation failure, id: ${source[0].id.logId} Encountered unexpected token: "broken" <S_IDENTIFIER>"""))
        verify(reportingContext).warning(eq("""Message transformation failure, id: ${source[1].id.logId} Unsupported operation kind 'broken operation'"""))
    }

    @Test
    fun `decode broken row message test`() {
        val config = LogMinerConfiguration()
        val codec = LogMinerTransformer(config)
        val source: ParsedMessage =
            ParsedMessage.builder()
                .setId(
                    MessageId.builder()
                        .setSessionAlias("test-session-alias")
                        .setDirection(Direction.OUTGOING)
                        .setTimestamp(Instant.now())
                        .setSequence(1)
                        .build()
                )
                .setBody(
                    mapOf(
                        "OPERATION" to "INSERT",
                        "SQL_REDO" to "broken query",
                        "ROW_ID" to 1,
                        "TIMESTAMP" to Instant.now().toString(),
                        "TABLE_NAME" to "test-table",
                    )
                )
                .setType("test-message")
                .setProtocol("")
                .build()

        assertThrows<JSQLParserException> {
            codec.decode(source.toGroup(), reportingContext)
        }
    }

    @Test
    fun `toReadable test`() {
        val onWarning: (String) -> Unit = mock { }
        val insert = CCJSqlParserUtil.parse(
            """
            insert into "OWNER"."table"("NAME","TIMESTAMP","DATE","NUMBER","NULL") 
            values ('An',TO_TIMESTAMP('12-DEC-23 02.55.01 PM'), TO_DATE('12-DEC-23', 'DD-MON-RR'), 8, NULL);
        """.trimIndent()
        ) as Insert
        val result: List<Any?> = insert.select.values.expressions.map { it.toReadable(onWarning) }
        expectThat(result) {
            hasSize(5)
            withElementAt(0) { isEqualTo("An") }
            withElementAt(1) {
                isEqualTo(
                    hashMapOf(
                        "function" to "TO_TIMESTAMP",
                        "parameters" to listOf("12-DEC-23 02.55.01 PM")
                    )
                )
            }
            withElementAt(2) {
                isEqualTo(
                    hashMapOf(
                        "function" to "TO_DATE",
                        "parameters" to listOf("12-DEC-23", "DD-MON-RR")
                    )
                )
            }
            withElementAt(3) { isEqualTo(8L) }
            withElementAt(4) { isNull() }
        }
        verifyNoMoreInteractions(onWarning)
    }

    private fun loadMessages(): List<ParsedMessage> {
        return LogMinerTransformerTest::class.java.getResourceAsStream(
            "/com/exactpro/th2/codec/oracle/logminer/log_miner.csv"
        ).use { inputStream ->
            requireNotNull(inputStream) {
                "'log_miner.csv' resource doesn't exist"
            }
            csvReader().open(inputStream) {
                readAllWithHeaderAsSequence()
                    .mapIndexed { index, row ->
                        ParsedMessage.builder().apply {
                            idBuilder()
                                .setSessionAlias("test-session-alias")
                                .setSequence(index.toLong())
                                .setDirection(Direction.INCOMING)
                                .setTimestamp(Instant.now())
                            setBody(row)
                            setType("test-type")
                            setProtocol("")
                        }.build()
                    }.toList()
            }
        }
    }
}