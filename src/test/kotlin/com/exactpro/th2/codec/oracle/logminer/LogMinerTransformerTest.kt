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

import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.api.impl.ReportingContext
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.transport.toGroup
import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import strikt.api.expectThat
import strikt.assertions.filterIsInstance
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.single
import java.time.Instant

class LogMinerTransformerTest {
    private val reportingContext: IReportingContext = ReportingContext()

    @Test
    fun decodesDataUsingDefaultHeader() {
        val config = LogMinerConfiguration()
        val codec = LogMinerTransformer(config)
        val sourceMessages: List<ParsedMessage> = loadMessages()
        assertEquals(4, sourceMessages.size)

        sourceMessages[0].let { source ->
            assertEquals(67, source.body.size)
            expectThat(codec.decode(source.toGroup(), reportingContext).messages).hasSize(1)
                .filterIsInstance<ParsedMessage>().apply {
                    hasSize(1)
                    single().apply {
                        get { id }.isEqualTo(source.id)
                        get { eventId }.isEqualTo(source.eventId)
                        get { type }.isEqualTo("th2-codec-error")
                        get { protocol }.isEqualTo("[csv,oracle-log-miner]")
                        get { body }.isEqualTo(
                            mapOf("content" to "Unsupported operation kind 'DDL'")
                                    + source.body.filterKeys { config.saveColumns.contains(it) }
                        )
                    }
                }
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

    private fun loadMessages(): List<ParsedMessage> {
        return LogMinerTransformerTest::class.java.getResourceAsStream(
            "/com/exactpro/th2/codec/oracle/logminer/log_miner.csv"
        ).use { inputStream ->
            requireNotNull(inputStream)
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
                        }.build()
                    }.toList()
            }
        }
    }
}