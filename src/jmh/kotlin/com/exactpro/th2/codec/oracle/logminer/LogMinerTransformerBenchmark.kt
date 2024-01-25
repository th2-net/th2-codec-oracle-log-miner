/*
 * Copyright 2024 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.codec.oracle.logminer.antlr.PlSqlLexer
import com.exactpro.th2.codec.oracle.logminer.antlr.PlSqlParser
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.UpdateListener
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.expression.Function
import net.sf.jsqlparser.expression.LongValue
import net.sf.jsqlparser.expression.NullValue
import net.sf.jsqlparser.expression.StringValue
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import net.sf.jsqlparser.statement.insert.Insert
import net.sf.jsqlparser.statement.update.Update
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.tree.ParseTreeWalker
import org.openjdk.jmh.annotations.Benchmark
import org.openjdk.jmh.annotations.BenchmarkMode
import org.openjdk.jmh.annotations.Mode.Throughput
import org.openjdk.jmh.annotations.Scope
import org.openjdk.jmh.annotations.Scope.Thread
import org.openjdk.jmh.annotations.State
import org.openjdk.jmh.infra.Blackhole

@State(Scope.Benchmark)
open class LogMinerTransformerBenchmark {

    private val context = object : IReportingContext {
        override fun warning(message: String) {
            // do nothing
        }

        override fun warnings(messages: Iterable<String>) {
            // do nothing
        }

    }

    @State(Thread)
    open class Messages {
        val shortInsertSql = """INSERT INTO "OWNER"."test-table"("f-00") 
                                VALUES ('v-00');"""
        val mediumInsertSql = """INSERT INTO "OWNER"."test-table" (
            "v-00", "v-01", "v-02", "v-03", "v-04", "v-05", "v-06", "v-07", "v-08", "v-09",
            "v-10", "v-11", "v-12", "v-13", "v-14", "v-15", "v-16", "v-17", "v-18", "v-19"
                                ) VALUES (
            'v-00', 'v-01', 'v-02', 'v-03', 'v-04', 'v-05', 'v-06', 'v-07', 'v-08', 'v-09',
            'v-10', 'v-11', 'v-12', 'v-13', 'v-14', 'v-15', 'v-16', 'v-17', 'v-18', 'v-19'
                                );"""
        val longInsertSql = """INSERT INTO "OWNER"."test-table" (
            "v-00", "v-01", "v-02", "v-03", "v-04", "v-05", "v-06", "v-07", "v-08", "v-09",
            "v-10", "v-11", "v-12", "v-13", "v-14", "v-15", "v-16", "v-17", "v-18", "v-19",
            "v-20", "v-21", "v-22", "v-23", "v-24", "v-25", "v-26", "v-27", "v-28", "v-29",
            "v-30", "v-31", "v-32", "v-33", "v-34", "v-35", "v-36", "v-37", "v-38", "v-39",
            "v-40", "v-41", "v-42", "v-43", "v-44", "v-45", "v-46", "v-47", "v-48", "v-49",
            "v-50", "v-51", "v-52", "v-53", "v-54", "v-55", "v-56", "v-57", "v-58", "v-59",
            "v-70", "v-71", "v-72", "v-73", "v-74", "v-75", "v-76", "v-77", "v-78", "v-79",
            "v-80", "v-81", "v-82", "v-83", "v-84", "v-85", "v-86", "v-87", "v-88", "v-89",
            "v-90", "v-91", "v-92", "v-93", "v-94", "v-95", "v-96", "v-97", "v-98", "v-99"
                                ) VALUES (
            'v-00', 'v-01', 'v-02', 'v-03', 'v-04', 'v-05', 'v-06', 'v-07', 'v-08', 'v-09',
            'v-10', 'v-11', 'v-12', 'v-13', 'v-14', 'v-15', 'v-16', 'v-17', 'v-18', 'v-19',
            'v-20', 'v-21', 'v-22', 'v-23', 'v-24', 'v-25', 'v-26', 'v-27', 'v-28', 'v-29',
            'v-30', 'v-31', 'v-32', 'v-33', 'v-34', 'v-35', 'v-36', 'v-37', 'v-38', 'v-39',
            'v-40', 'v-41', 'v-42', 'v-43', 'v-44', 'v-45', 'v-46', 'v-47', 'v-48', 'v-49',
            'v-50', 'v-51', 'v-52', 'v-53', 'v-54', 'v-55', 'v-56', 'v-57', 'v-58', 'v-59',
            'v-70', 'v-71', 'v-72', 'v-73', 'v-74', 'v-75', 'v-76', 'v-77', 'v-78', 'v-79',
            'v-80', 'v-81', 'v-82', 'v-83', 'v-84', 'v-85', 'v-86', 'v-87', 'v-88', 'v-89',
            'v-90', 'v-91', 'v-92', 'v-93', 'v-94', 'v-95', 'v-96', 'v-97', 'v-98', 'v-99'
                                );"""
        val shortUpdateSql = """UPDATE "OWNER"."test-table" SET "f-00" = 'v-00'
                                WHERE "f-00" = 'v-00';"""
        val mediumUpdateSql = """UPDATE "OWNER"."test-table" SET
            "f-00" = 'v-00', "f-01" = 'v-01', "f-02" = 'v-02', "f-03" = 'v-03', "f-04" = 'v-04', "f-05" = 'v-05', "f-06" = 'v-06', "f-07" = 'v-07', "f-08" = 'v-08', "f-09" = 'v-09',
            "f-10" = 'v-10', "f-11" = 'v-11', "f-12" = 'v-12', "f-13" = 'v-13', "f-14" = 'v-14', "f-15" = 'v-15', "f-16" = 'v-16', "f-17" = 'v-17', "f-18" = 'v-18', "f-19" = 'v-19'
                                WHERE
            "f-00" = 'v-00' and  "f-01" = 'v-01' and  "f-02" = 'v-02' and  "f-03" = 'v-03' and  "f-04" = 'v-04' and  "f-05" = 'v-05' and  "f-06" = 'v-06' and  "f-07" = 'v-07' and  "f-08" = 'v-08' and  "f-09" = 'v-09' and
            "f-10" = 'v-10' and  "f-11" = 'v-11' and  "f-12" = 'v-12' and  "f-13" = 'v-13' and  "f-14" = 'v-14' and  "f-15" = 'v-15' and  "f-16" = 'v-16' and  "f-17" = 'v-17' and  "f-18" = 'v-18' and  "f-19" = 'v-19'
                                ;"""
        val longUpdateSql = """UPDATE "OWNER"."test-table" SET
            "f-00" = 'v-00', "f-01" = 'v-01', "f-02" = 'v-02', "f-03" = 'v-03', "f-04" = 'v-04', "f-05" = 'v-05', "f-06" = 'v-06', "f-07" = 'v-07', "f-08" = 'v-08', "f-09" = 'v-09',
            "f-10" = 'v-10', "f-11" = 'v-11', "f-12" = 'v-12', "f-13" = 'v-13', "f-14" = 'v-14', "f-15" = 'v-15', "f-16" = 'v-16', "f-17" = 'v-17', "f-18" = 'v-18', "f-19" = 'v-19',
            "f-20" = 'v-20', "f-21" = 'v-21', "f-22" = 'v-22', "f-23" = 'v-23', "f-24" = 'v-24', "f-25" = 'v-25', "f-26" = 'v-26', "f-27" = 'v-27', "f-28" = 'v-28', "f-29" = 'v-29',
            "f-30" = 'v-30', "f-31" = 'v-31', "f-32" = 'v-32', "f-33" = 'v-33', "f-34" = 'v-34', "f-35" = 'v-35', "f-36" = 'v-36', "f-37" = 'v-37', "f-38" = 'v-38', "f-39" = 'v-39',
            "f-40" = 'v-40', "f-41" = 'v-41', "f-42" = 'v-42', "f-43" = 'v-43', "f-44" = 'v-44', "f-45" = 'v-45', "f-46" = 'v-46', "f-47" = 'v-47', "f-48" = 'v-48', "f-49" = 'v-49',
            "f-50" = 'v-50', "f-51" = 'v-51', "f-52" = 'v-52', "f-53" = 'v-53', "f-54" = 'v-54', "f-55" = 'v-55', "f-56" = 'v-56', "f-57" = 'v-57', "f-58" = 'v-58', "f-59" = 'v-59',
            "f-60" = 'v-60', "f-61" = 'v-61', "f-62" = 'v-62', "f-63" = 'v-63', "f-64" = 'v-64', "f-65" = 'v-65', "f-66" = 'v-66', "f-67" = 'v-67', "f-68" = 'v-68', "f-69" = 'v-69',
            "f-70" = 'v-70', "f-71" = 'v-71', "f-72" = 'v-72', "f-73" = 'v-73', "f-74" = 'v-74', "f-75" = 'v-75', "f-76" = 'v-76', "f-77" = 'v-77', "f-78" = 'v-78', "f-79" = 'v-79',
            "f-80" = 'v-80', "f-81" = 'v-81', "f-82" = 'v-82', "f-83" = 'v-83', "f-84" = 'v-84', "f-85" = 'v-85', "f-86" = 'v-86', "f-87" = 'v-87', "f-88" = 'v-88', "f-89" = 'v-89',
            "f-90" = 'v-90', "f-91" = 'v-91', "f-92" = 'v-92', "f-93" = 'v-93', "f-94" = 'v-94', "f-95" = 'v-95', "f-96" = 'v-96', "f-97" = 'v-97', "f-98" = 'v-98', "f-99" = 'v-99'
                                WHERE
            "f-00" = 'v-00' and  "f-01" = 'v-01' and  "f-02" = 'v-02' and  "f-03" = 'v-03' and  "f-04" = 'v-04' and  "f-05" = 'v-05' and  "f-06" = 'v-06' and  "f-07" = 'v-07' and  "f-08" = 'v-08' and  "f-09" = 'v-09' and
            "f-10" = 'v-10' and  "f-11" = 'v-11' and  "f-12" = 'v-12' and  "f-13" = 'v-13' and  "f-14" = 'v-14' and  "f-15" = 'v-15' and  "f-16" = 'v-16' and  "f-17" = 'v-17' and  "f-18" = 'v-18' and  "f-19" = 'v-19' and
            "f-20" = 'v-20' and  "f-21" = 'v-21' and  "f-22" = 'v-22' and  "f-23" = 'v-23' and  "f-24" = 'v-24' and  "f-25" = 'v-25' and  "f-26" = 'v-26' and  "f-27" = 'v-27' and  "f-28" = 'v-28' and  "f-29" = 'v-29' and
            "f-30" = 'v-30' and  "f-31" = 'v-31' and  "f-32" = 'v-32' and  "f-33" = 'v-33' and  "f-34" = 'v-34' and  "f-35" = 'v-35' and  "f-36" = 'v-36' and  "f-37" = 'v-37' and  "f-38" = 'v-38' and  "f-39" = 'v-39' and
            "f-40" = 'v-40' and  "f-41" = 'v-41' and  "f-42" = 'v-42' and  "f-43" = 'v-43' and  "f-44" = 'v-44' and  "f-45" = 'v-45' and  "f-46" = 'v-46' and  "f-47" = 'v-47' and  "f-48" = 'v-48' and  "f-49" = 'v-49' and
            "f-50" = 'v-50' and  "f-51" = 'v-51' and  "f-52" = 'v-52' and  "f-53" = 'v-53' and  "f-54" = 'v-54' and  "f-55" = 'v-55' and  "f-56" = 'v-56' and  "f-57" = 'v-57' and  "f-58" = 'v-58' and  "f-59" = 'v-59' and
            "f-60" = 'v-60' and  "f-61" = 'v-61' and  "f-62" = 'v-62' and  "f-63" = 'v-63' and  "f-64" = 'v-64' and  "f-65" = 'v-65' and  "f-66" = 'v-66' and  "f-67" = 'v-67' and  "f-68" = 'v-68' and  "f-69" = 'v-69' and
            "f-70" = 'v-70' and  "f-71" = 'v-71' and  "f-72" = 'v-72' and  "f-73" = 'v-73' and  "f-74" = 'v-74' and  "f-75" = 'v-75' and  "f-76" = 'v-76' and  "f-77" = 'v-77' and  "f-78" = 'v-78' and  "f-79" = 'v-79' and
            "f-80" = 'v-80' and  "f-81" = 'v-81' and  "f-82" = 'v-82' and  "f-83" = 'v-83' and  "f-84" = 'v-84' and  "f-85" = 'v-85' and  "f-86" = 'v-86' and  "f-87" = 'v-87' and  "f-88" = 'v-88' and  "f-89" = 'v-89' and
            "f-90" = 'v-90' and  "f-91" = 'v-91' and  "f-92" = 'v-92' and  "f-93" = 'v-93' and  "f-94" = 'v-94' and  "f-95" = 'v-95' and  "f-96" = 'v-96' and  "f-97" = 'v-97' and  "f-98" = 'v-98' and  "f-99" = 'v-99'
                                ;"""
        val shortUpdateSqlWithoutWhere = """UPDATE "OWNER"."test-table" SET "f-00" = 'v-00';"""
        val mediumUpdateSqlWithoutWhere = """UPDATE "OWNER"."test-table" SET
            "f-00" = 'v-00', "f-01" = 'v-01', "f-02" = 'v-02', "f-03" = 'v-03', "f-04" = 'v-04', "f-05" = 'v-05', "f-06" = 'v-06', "f-07" = 'v-07', "f-08" = 'v-08', "f-09" = 'v-09',
            "f-10" = 'v-10', "f-11" = 'v-11', "f-12" = 'v-12', "f-13" = 'v-13', "f-14" = 'v-14', "f-15" = 'v-15', "f-16" = 'v-16', "f-17" = 'v-17', "f-18" = 'v-18', "f-19" = 'v-19'
                                ;"""
        val longUpdateSqlWithoutWhere = """UPDATE "OWNER"."test-table" SET
            "f-00" = 'v-00', "f-01" = 'v-01', "f-02" = 'v-02', "f-03" = 'v-03', "f-04" = 'v-04', "f-05" = 'v-05', "f-06" = 'v-06', "f-07" = 'v-07', "f-08" = 'v-08', "f-09" = 'v-09',
            "f-10" = 'v-10', "f-11" = 'v-11', "f-12" = 'v-12', "f-13" = 'v-13', "f-14" = 'v-14', "f-15" = 'v-15', "f-16" = 'v-16', "f-17" = 'v-17', "f-18" = 'v-18', "f-19" = 'v-19',
            "f-20" = 'v-20', "f-21" = 'v-21', "f-22" = 'v-22', "f-23" = 'v-23', "f-24" = 'v-24', "f-25" = 'v-25', "f-26" = 'v-26', "f-27" = 'v-27', "f-28" = 'v-28', "f-29" = 'v-29',
            "f-30" = 'v-30', "f-31" = 'v-31', "f-32" = 'v-32', "f-33" = 'v-33', "f-34" = 'v-34', "f-35" = 'v-35', "f-36" = 'v-36', "f-37" = 'v-37', "f-38" = 'v-38', "f-39" = 'v-39',
            "f-40" = 'v-40', "f-41" = 'v-41', "f-42" = 'v-42', "f-43" = 'v-43', "f-44" = 'v-44', "f-45" = 'v-45', "f-46" = 'v-46', "f-47" = 'v-47', "f-48" = 'v-48', "f-49" = 'v-49',
            "f-50" = 'v-50', "f-51" = 'v-51', "f-52" = 'v-52', "f-53" = 'v-53', "f-54" = 'v-54', "f-55" = 'v-55', "f-56" = 'v-56', "f-57" = 'v-57', "f-58" = 'v-58', "f-59" = 'v-59',
            "f-60" = 'v-60', "f-61" = 'v-61', "f-62" = 'v-62', "f-63" = 'v-63', "f-64" = 'v-64', "f-65" = 'v-65', "f-66" = 'v-66', "f-67" = 'v-67', "f-68" = 'v-68', "f-69" = 'v-69',
            "f-70" = 'v-70', "f-71" = 'v-71', "f-72" = 'v-72', "f-73" = 'v-73', "f-74" = 'v-74', "f-75" = 'v-75', "f-76" = 'v-76', "f-77" = 'v-77', "f-78" = 'v-78', "f-79" = 'v-79',
            "f-80" = 'v-80', "f-81" = 'v-81', "f-82" = 'v-82', "f-83" = 'v-83', "f-84" = 'v-84', "f-85" = 'v-85', "f-86" = 'v-86', "f-87" = 'v-87', "f-88" = 'v-88', "f-89" = 'v-89',
            "f-90" = 'v-90', "f-91" = 'v-91', "f-92" = 'v-92', "f-93" = 'v-93', "f-94" = 'v-94', "f-95" = 'v-95', "f-96" = 'v-96', "f-97" = 'v-97', "f-98" = 'v-98', "f-99" = 'v-99'
                                ;"""
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortInsertJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertJSQL(blackHole, state.shortInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumInsertJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertJSQL(blackHole, state.mediumInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongInsertJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertJSQL(blackHole, state.longInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortUpdateJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.shortUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumUpdateJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.mediumUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongUpdateJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.longUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortUpdateWithoutWhereJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.shortUpdateSqlWithoutWhere)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumUpdateWithoutWhereJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.mediumUpdateSqlWithoutWhere)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongUpdateWithoutWhereJSQLBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateJSQL(blackHole, state.longUpdateSqlWithoutWhere)
    }



    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortInsertAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertAntlr(blackHole, state.shortInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumInsertAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertAntlr(blackHole, state.mediumInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongInsertAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseInsertAntlr(blackHole, state.longInsertSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortUpdateAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.shortUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumUpdateAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.mediumUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongUpdateAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.longUpdateSql)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseShortUpdateWithoutWhereAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.shortUpdateSqlWithoutWhere)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseMediumUpdateWithoutWhereAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.mediumUpdateSqlWithoutWhere)
    }

    @Benchmark
    @BenchmarkMode(Throughput)
    fun parseLongUpdateWithoutWhereAntlrBenchmark(state: Messages, blackHole: Blackhole) {
        parseUpdateAntlr(blackHole, state.longUpdateSqlWithoutWhere)
    }

    private fun parseInsertJSQL(
        blackHole: Blackhole,
        sql: String
    ) {
        blackHole.consume(
            buildMap {
                val insert = CCJSqlParserUtil.parse(sql) as Insert
                insert.columns.forEachIndexed { index, column ->
                    put(
                        column.columnName,
                        insert.select.values.expressions[index].toReadable(context::warning)
                    )
                }
            }
        )
    }

    private fun parseUpdateJSQL(
        blackHole: Blackhole,
        sql: String
    ) {
        blackHole.consume(
            buildMap {
                val update = CCJSqlParserUtil.parse(sql) as Update
                update.updateSets.forEach {
                    put(
                        it.columns.single().columnName.trim('"'),
                        it.values.single().toReadable(context::warning)
                    )
                }
            }
        )
    }

    private fun parseInsertAntlr(
        blackHole: Blackhole,
        sql: String
    ) {
        blackHole.consume(
            MapBuilder<String, Any?>().apply {
                val lexer = PlSqlLexer(CharStreams.fromString(sql))
                val tokens = CommonTokenStream(lexer)
                val parser = PlSqlParser(tokens)
                val walker = ParseTreeWalker()
                walker.walk(InsertListener(this, ""), parser.insert_statement())
            }
        )
    }

    private fun parseUpdateAntlr(
        blackHole: Blackhole,
        sql: String
    ) {
        blackHole.consume(
            MapBuilder<String, Any?>().apply {
                val lexer = PlSqlLexer(CharStreams.fromString(sql))
                val tokens = CommonTokenStream(lexer)
                val parser = PlSqlParser(tokens)
                val walker = ParseTreeWalker()
                walker.walk(UpdateListener(this, ""), parser.update_statement())
            }
        )
    }

    private fun Function.toReadable(onWarning: (String) -> Unit): Map<String, Any?> {
        return hashMapOf(
            "function" to name,
            "parameters" to parameters.map { it.toReadable(onWarning) }
        )
    }

    private fun Expression.toReadable(onWarning: (String) -> Unit): Any? {
        return when (this) {
            is Function -> toReadable(onWarning)
            is StringValue -> value.trim()
            is LongValue -> value
            is NullValue -> null
            else -> {
                onWarning("Unsupported expression: '$this', type: ${this::class.java}")
                toString()
            }
        }
    }
}