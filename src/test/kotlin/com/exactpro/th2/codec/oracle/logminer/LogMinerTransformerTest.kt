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
import com.exactpro.th2.codec.oracle.logminer.LogMinerTransformer.Companion.truncateFromWhereClause
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.UpdateListener
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import com.exactpro.th2.common.utils.message.transport.logId
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyNoMoreInteractions
import strikt.api.expectThat
import strikt.assertions.filterIsInstance
import strikt.assertions.hasSize
import strikt.assertions.isA
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
                        get { body }.isA<Map<String, Any?>>().and {
                            hasSize(12 + config.saveColumns.size)
                            get { get("th2_SALARY") }.isEqualTo("110,000 ")
                            get { get("th2_DENTAL_PLAN") }.isEqualTo("Delta Dental ")
                            get { get("th2_ID") }.isEqualTo("1")
                            get { get("th2_SICK_TIME") }.isEqualTo("5")
                            get { get("th2_PLAN") }.isEqualTo("25000")
                            get { get("th2_TITLE") }.isEqualTo("Manager ")
                            get { get("th2_TIME_OFF") }.isEqualTo("15")
                            get { get("th2_HEALTH_PLAN") }.isEqualTo("Blue Cross and Blue Shield ")
                            get { get("th2_NAME") }.isEqualTo("Chris Montgomery ")
                            get { get("th2_VISION_PLAN") }.isEqualTo("Aetna Vision ")
                            get { get("th2_SAVINGS") }.isEqualTo("1")
                            get { get("th2_BONUS_STRUCTURE") }.isEqualTo("5% Quarterly ")

                            source.body.forEach { (key, value) ->
                                if (config.saveColumns.contains(key)) {
                                    get { get(key) }.isEqualTo(value)
                                }
                            }
                        }
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
                        "OPERATION" to "UPDATE",
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
                .setId(baseId.copy(subsequence = listOf(3)))
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
        ).hasSize(3)
            .filterIsInstance<ParsedMessage>().and {
                hasSize(3)
                withElementAt(0) {
                    val message = source[0]
                    get { id }.isEqualTo(message.id)
                    get { eventId }.isEqualTo(message.eventId)
                    get { type }.isEqualTo("th2-codec-error")
                    get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                    get { body }.isEqualTo(
                        mapOf(
                            "content" to """Parse problem(s): [line 1:0 mismatched input 'broken' expecting 'INSERT']"""
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
                            "content" to """Parse problem(s): [line 1:0 missing 'UPDATE' at 'broken', line 1:12 mismatched input '<EOF>' expecting 'SET']"""
                        ) + message.body.filterKeys { config.saveColumns.contains(it) }
                    )
                }
                withElementAt(2) {
                    val message = source[2]
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
        verify(reportingContext).warning(eq("""Message transformation failure, id: ${source[0].id.logId} Parse problem(s): [line 1:0 mismatched input 'broken' expecting 'INSERT']"""))
        verify(reportingContext).warning(eq("""Message transformation failure, id: ${source[1].id.logId} Parse problem(s): [line 1:0 missing 'UPDATE' at 'broken', line 1:12 mismatched input '<EOF>' expecting 'SET']"""))
        verify(reportingContext).warning(eq("""Message transformation failure, id: ${source[2].id.logId} Unsupported operation kind 'broken operation'"""))
    }

    @Test
    fun `decode broken insert query test`() {
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

        assertThrows<IllegalStateException> {
            codec.decode(source.toGroup(), reportingContext)
        }.also {
            assertEquals(it.message, "Parse problem(s): [line 1:0 mismatched input 'broken' expecting 'INSERT']")
        }
    }

    @Test
    fun `decode broken update query test`() {
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
                        "OPERATION" to "UPDATE",
                        "SQL_REDO" to "broken query",
                        "ROW_ID" to 1,
                        "TIMESTAMP" to Instant.now().toString(),
                        "TABLE_NAME" to "test-table",
                    )
                )
                .setType("test-message")
                .setProtocol("")
                .build()

        assertThrows<IllegalStateException> {
            codec.decode(source.toGroup(), reportingContext)
        }.also {
            assertEquals(
                "Parse problem(s): [line 1:0 missing 'UPDATE' at 'broken', line 1:12 mismatched input '<EOF>' expecting 'SET']",
                it.message
            )
        }
    }

    @Test
    fun `insert parser test`() {
        val query = """
                INSERT INTO "OWNER"."table"("NAME","TIMESTAMP","DATE","NUMBER","FLOAT","NULL","NESTED") 
                VALUES ('An',TO_TIMESTAMP('12-DEC-23 02.55.01 PM'), TO_DATE('12-DEC-23', 'DD-MON-RR'), 8, 1.1, NULL, TO_TIMESTAMP(TO_TIMESTAMP('12-DEC-23 02.55.01 PM'))); 
            """
        val builder = MapBuilder<String, Any?>()
        InsertListener.parse(builder, TEST_PREFIX, query)

        expectThat(builder.build()) {
            hasSize(6)
            get { get("${TEST_PREFIX}NAME") }.isEqualTo("An")
            get { get("${TEST_PREFIX}NUMBER") }.isEqualTo(8L)
            get { get("${TEST_PREFIX}FLOAT") }.isEqualTo(1.1)
            get { get("${TEST_PREFIX}TIMESTAMP") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_TIMESTAMP")
                get { get("parameters") }.isEqualTo(listOf("12-DEC-23 02.55.01 PM"))
            }
            get { get("${TEST_PREFIX}DATE") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_DATE")
                get { get("parameters") }.isEqualTo(listOf("12-DEC-23", "DD-MON-RR"))
            }
            get { get("${TEST_PREFIX}NESTED") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_TIMESTAMP")
                get { get("parameters") }.isA<List<Map<String, Any>>>().single().and {
                    hasSize(2)
                    get { get("function") }.isEqualTo("TO_TIMESTAMP")
                    get { get("parameters") }.isEqualTo(listOf("12-DEC-23 02.55.01 PM"))
                }
            }
        }
    }

    @Test
    fun `update parser test`() {
        val query = """
                UPDATE "OWNER"."table" SET 
                  "NAME" = 'An', 
                  "TIMESTAMP" = TO_TIMESTAMP('12-DEC-23 02.55.01 PM'),
                  "DATE" = TO_DATE('12-DEC-23', 'DD-MON-RR'),
                  "NUMBER" = 8,
                  "FLOAT" = 1.1,
                  "NULL" = NULL,
                  "NESTED" = TO_TIMESTAMP(TO_TIMESTAMP('12-DEC-23 02.55.01 PM'))
                WHERE 
                  ROWID = 'test-row-id';
            """.trimIndent()
        val builder = MapBuilder<String, Any?>()
        UpdateListener.parse(builder, TEST_PREFIX, query)

        expectThat(builder.build()) {
            hasSize(7)
            get { get("${TEST_PREFIX}NAME") }.isEqualTo("An")
            get { get("${TEST_PREFIX}NUMBER") }.isEqualTo(8L)
            get { get("${TEST_PREFIX}FLOAT") }.isEqualTo(1.1)
            get { get("${TEST_PREFIX}NULL") }.isNull()
            get { get("${TEST_PREFIX}TIMESTAMP") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_TIMESTAMP")
                get { get("parameters") }.isEqualTo(listOf("12-DEC-23 02.55.01 PM"))
            }
            get { get("${TEST_PREFIX}DATE") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_DATE")
                get { get("parameters") }.isEqualTo(listOf("12-DEC-23", "DD-MON-RR"))
            }
            get { get("${TEST_PREFIX}NESTED") }.isA<Map<String, Any>>().and {
                hasSize(2)
                get { get("function") }.isEqualTo("TO_TIMESTAMP")
                get { get("parameters") }.isA<List<Map<String, Any>>>().single().and {
                    hasSize(2)
                    get { get("function") }.isEqualTo("TO_TIMESTAMP")
                    get { get("parameters") }.isEqualTo(listOf("12-DEC-23 02.55.01 PM"))
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource(
        """abcWHEREcde,abc;""",
        """abc WHERE cde,abc ;""",
        """abcwherecde,abc;""",
        """abc where cde,abc ;""",

    )
    fun `truncateFromWhereClause test`(source: String, target: String) {
        assertEquals(target, truncateFromWhereClause(source))
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

    companion object {
        private const val TEST_PREFIX = "test-prefix-"
    }
}