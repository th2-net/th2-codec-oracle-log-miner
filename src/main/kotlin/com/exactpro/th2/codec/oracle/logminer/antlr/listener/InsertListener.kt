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

package com.exactpro.th2.codec.oracle.logminer.antlr.listener

import com.exactpro.th2.codec.oracle.logminer.antlr.PlSqlParser
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener.Stage.BEGIN
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener.Stage.PARSED_NAMES
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener.Stage.PARSED_VALUES
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener.Stage.PARSING_NAMES
import com.exactpro.th2.codec.oracle.logminer.antlr.listener.InsertListener.Stage.PARSING_VALUES
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import mu.KotlinLogging
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ParseTree
import java.util.LinkedList
import java.util.Queue

internal class InsertListener private constructor(
    builder: MapBuilder<String, Any?>,
    prefix: String,
    trimContent: Boolean,
    query: String
) : AbstractListener(
    builder,
    prefix,
    trimContent,
    query,
) {
    private val columNames: Queue<String> = LinkedList()
    private var stage = BEGIN

    override fun enterInsert_statement(ctx: PlSqlParser.Insert_statementContext) {
        checkStage(ctx, "enterInsert_statement", BEGIN)

        check(columNames.isEmpty()) {
            "Column names must be empty, actual: $columNames"
        }
        check(expressionHolder.isEmpty()) {
            "Expression holder must be empty, actual: $expressionHolder"
        }
    }

    override fun enterInsert_into_clause(ctx: PlSqlParser.Insert_into_clauseContext) {
        checkStage(ctx, "enterInsert_into_clause", BEGIN)
        stage = PARSING_NAMES
    }

    override fun enterColumn_name(ctx: PlSqlParser.Column_nameContext) {
        checkStage(ctx, "enterColumn_name", PARSING_NAMES)
        columNames.add(ctx.text.removeSurrounding("\""))
    }

    override fun exitInsert_into_clause(ctx: PlSqlParser.Insert_into_clauseContext) {
        checkStage(ctx, "exitInsert_into_clause", PARSING_NAMES)
        stage = PARSED_NAMES
    }

    override fun enterValues_clause(ctx: PlSqlParser.Values_clauseContext) {
        checkStage(ctx, "enterValues_clause", PARSED_NAMES)
        stage = PARSING_VALUES
    }

    override fun enterUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
        checkStage(ctx, "enterUnary_expression", PARSING_VALUES)
        super.enterUnary_expression(ctx)
    }

    override fun enterConstant(ctx: PlSqlParser.ConstantContext) {
        checkStage(ctx, "enterConstant", PARSING_VALUES)
        super.enterConstant(ctx)
    }

    override fun enterQuoted_string(ctx: PlSqlParser.Quoted_stringContext) {
        checkStage(ctx, "enterQuoted_string", PARSING_VALUES)
        super.enterQuoted_string(ctx)
    }

    override fun enterOther_function(ctx: PlSqlParser.Other_functionContext) {
        checkStage(ctx, "enterOther_function", PARSING_VALUES)
        super.enterOther_function(ctx)
    }

    override fun exitOther_function(ctx: PlSqlParser.Other_functionContext) {
        checkStage(ctx, "exitOther_function", PARSING_VALUES)
        super.exitOther_function(ctx)
    }

    override fun enterString_function(ctx: PlSqlParser.String_functionContext) {
        checkStage(ctx, "enterString_function", PARSING_VALUES)
        super.enterString_function(ctx)
    }

    override fun exitString_function(ctx: PlSqlParser.String_functionContext) {
        checkStage(ctx, "exitString_function", PARSING_VALUES)
        super.exitString_function(ctx)
    }

    override fun exitUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
        checkStage(ctx, "exitUnary_expression", PARSING_VALUES)

        val expression = handle(ctx)

        when(expressionHolder.size) {
            0 -> {
                val value = expression.value
                val column = requireNotNull(columNames.poll()) {
                    "Column name isn't specified for $value value"
                }
                if (value != null) {
                    putValue(column, value)
                }
                expressionHolder.clear()
                LOGGER.trace { "Handled '$column' to '$value' pair" }
            }
            else -> expressionHolder.last().appendValue(expression.value)
        }
    }

    override fun exitValues_clause(ctx: PlSqlParser.Values_clauseContext) {
        checkStage(ctx, "exitValues_clause", PARSING_VALUES)
        stage = PARSED_VALUES
    }

    override fun enterMulti_table_insert(ctx: PlSqlParser.Multi_table_insertContext) {
        LOGGER.trace { "enterMulti_table_insert token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
        error("Unsupported multiple table insert ${ctx.text}")
    }

    override fun exitInsert_statement(ctx: PlSqlParser.Insert_statementContext) {
        checkStage(ctx, "exitInsert_statement", PARSED_VALUES)
        check(columNames.isEmpty()) {
            "Incorrect stage for parsing insert statement, unused column names: $columNames, text: ${ctx.text}"
        }
        check(expressionHolder.isEmpty()) {
            "Incorrect stage for parsing insert statement, uncompleted values: $expressionHolder, text: ${ctx.text}"
        }
    }

    override fun PlSqlParser.parseTree(): ParseTree = insert_statement()

    private fun checkStage(ctx: ParserRuleContext, methodName: String, expected: Stage) {
        checkError(ctx, methodName)
        check(stage == expected) {
            "Incorrect stage for $methodName, expected: $expected, actual: $stage, text: ${ctx.text}"
        }
    }

    private enum class Stage {
        BEGIN,
        PARSING_NAMES,
        PARSED_NAMES,
        PARSING_VALUES,
        PARSED_VALUES,
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        fun parse(
            builder: MapBuilder<String, Any?>,
            prefix: String,
            trimContent: Boolean,
            query: String,
        ) = InsertListener(builder, prefix, trimContent, query).parse()
    }
}