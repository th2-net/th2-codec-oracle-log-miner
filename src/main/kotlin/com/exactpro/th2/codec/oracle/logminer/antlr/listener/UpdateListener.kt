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
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import mu.KotlinLogging
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ParseTree

internal class UpdateListener private constructor(
    builder: MapBuilder<String, Any?>,
    prefix: String,
    trimContent: Boolean,
    query: String
) : AbstractListener(
    builder,
    prefix,
    trimContent,
    query
) {
    private var rowValue: RowValue? = null

    override fun enterUpdate_statement(ctx: PlSqlParser.Update_statementContext) {
        checkError(ctx, "enterUpdate_statement")

        check(expressionHolder.isEmpty()) {
            "Expression holder must be empty, actual: $expressionHolder"
        }
    }

    override fun enterColumn_based_update_set_clause(ctx: PlSqlParser.Column_based_update_set_clauseContext) {
        checkError(ctx, "enterColumn_based_update_set_clause")
        check(rowValue == null) {
            "Incorrect state for parsing column name: previous row value isn't null $rowValue, text: ${ctx.text}"
        }
        rowValue = RowValue(ctx.start.tokenIndex)
    }

    override fun enterColumn_name(ctx: PlSqlParser.Column_nameContext) {
        checkError(ctx, "enterColumn_name")
        requireNotNull(rowValue) {
            "Incorrect state for parsing column name: previous row value isn't null $rowValue, text: ${ctx.text}"
        }.apply {
            column = ctx.text.removeSurrounding("\"")
        }
    }

    override fun enterUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
        if (!checkStage(ctx, "enterUnary_expression")) return
        super.enterUnary_expression(ctx)
    }

    override fun enterConstant(ctx: PlSqlParser.ConstantContext) {
        if (!checkStage(ctx, "enterConstant")) return
        super.enterConstant(ctx)
    }

    override fun enterQuoted_string(ctx: PlSqlParser.Quoted_stringContext) {
        if (!checkStage(ctx, "enterQuoted_string")) return
        super.enterQuoted_string(ctx)
    }

    override fun enterOther_function(ctx: PlSqlParser.Other_functionContext) {
        if (!checkStage(ctx, "enterOther_function")) return
        super.enterOther_function(ctx)
    }

    override fun exitOther_function(ctx: PlSqlParser.Other_functionContext) {
        if (!checkStage(ctx, "exitOther_function")) return
        super.exitOther_function(ctx)
    }

    override fun enterString_function(ctx: PlSqlParser.String_functionContext) {
        if (!checkStage(ctx, "enterString_function")) return
        super.enterString_function(ctx)
    }

    override fun exitString_function(ctx: PlSqlParser.String_functionContext) {
        if (!checkStage(ctx, "exitString_function")) return
        super.exitString_function(ctx)
    }

    override fun exitUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
        if (!checkStage(ctx, "exitUnary_expression")) return

        val expression = handle(ctx)

        when(expressionHolder.size) {
            0 -> {
                rowValue?.value = expression.value
                expressionHolder.clear()
            }
            else -> expressionHolder.last().appendValue(expression.value)
        }
    }

    override fun exitColumn_based_update_set_clause(ctx: PlSqlParser.Column_based_update_set_clauseContext) {
        checkError(ctx, "exitColumn_based_update_set_clause")
        requireNotNull(rowValue) {
            "Incorrect state for parsing column name: current row value is null, text: ${ctx.text}"
        }.apply {
            check(isCompleted) {
                "Incorrect state for parsing column name: current row value isn't completed $this, text: ${ctx.text}"
            }

            putValue(column, value)
            rowValue = null
            LOGGER.trace { "Handled '$column' to '$value' pair" }
        }

    }

    override fun exitUpdate_statement(ctx: PlSqlParser.Update_statementContext) {
        checkError(ctx, "exitUpdate_statement")
        check(rowValue == null) {
            "Incorrect stage for parsing update statement, uncompleted row value pair: $rowValue, text: ${ctx.text}"
        }
        check(expressionHolder.isEmpty()) {
            "Incorrect stage for parsing update statement, uncompleted values: $expressionHolder, text: ${ctx.text}"
        }
    }

    override fun PlSqlParser.parseTree(): ParseTree = update_statement()

    /**
     * Use this check to verify stage of parsing
     * SET and WHERE blocks uses the same tokens
     */
    private fun checkStage(ctx: ParserRuleContext, methodName: String): Boolean {
        checkError(ctx, methodName)
        if (rowValue == null) {
            LOGGER.trace { "$methodName, text: ${ctx.text}, SKIPPED" }
            return false
        }
        return true
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}

        fun parse(
            builder: MapBuilder<String, Any?>,
            prefix: String,
            trimContent: Boolean,
            query: String,
        ) = UpdateListener(builder, prefix, trimContent, query).parse()
    }
}