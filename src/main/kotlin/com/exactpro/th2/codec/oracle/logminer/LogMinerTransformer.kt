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

import PlSqlLexer
import PlSqlParser
import PlSqlParserBaseListener
import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.api.IReportingContext
import com.exactpro.th2.codec.oracle.logminer.cfg.LogMinerConfiguration
import com.exactpro.th2.codec.util.ERROR_CONTENT_FIELD
import com.exactpro.th2.codec.util.ERROR_TYPE_MESSAGE
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.builders.MapBuilder
import com.exactpro.th2.common.utils.message.transport.logId
import mu.KotlinLogging
import org.antlr.v4.runtime.CharStreams
import org.antlr.v4.runtime.CommonTokenStream
import org.antlr.v4.runtime.ParserRuleContext
import org.antlr.v4.runtime.tree.ErrorNode
import org.antlr.v4.runtime.tree.ParseTreeWalker
import java.util.LinkedList
import java.util.Queue
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
                                    val lexer = PlSqlLexer(CharStreams.fromString(sqlRedo))
                                    val tokens = CommonTokenStream(lexer)
                                    val parser = PlSqlParser(tokens)
                                    val walker = ParseTreeWalker()
                                    walker.walk(InsertListener(this, config.columnPrefix), parser.insert_statement())
                                }
                            }
                        }

                        "UPDATE" -> {
                            message.toBuilderWithoutBody().apply {
                                bodyBuilder().apply {
                                    val preparedQuery = if (config.truncateUpdateQueryFromWhereClause) {
                                        truncateFromWhereClause(sqlRedo)
                                    } else {
                                        sqlRedo
                                    }
                                    val lexer = PlSqlLexer(CharStreams.fromString(preparedQuery))
                                    val tokens = CommonTokenStream(lexer)
                                    val parser = PlSqlParser(tokens)
                                    val walker = ParseTreeWalker()
                                    walker.walk(UpdateListener(this, config.columnPrefix), parser.update_statement())
                                }
                            }
                        }

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
            LOGGER.debug { "The ${message.id.logId} message isn't appropriate, type: '${message::class.java}', protocol: '${message.protocol}'" }
        }
    }

    // TODO: handle parsing exception
    class InsertListener(private val builder: MapBuilder<String, Any?>, private val prefix: String) : PlSqlParserBaseListener() {
        private val columNames: Queue<String> = LinkedList()
        private var stage = Stage.BEGIN
        private val expressionHolder: MutableList<Expression> = mutableListOf()

        override fun visitErrorNode(node: ErrorNode) {
            super.visitErrorNode(node)
        }

        override fun enterInsert_statement(ctx: PlSqlParser.Insert_statementContext) {
            LOGGER.trace { "enterInsert_statement token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }

            stage = Stage.BEGIN
            columNames.clear()
            expressionHolder.clear()
        }

        override fun enterInsert_into_clause(ctx: PlSqlParser.Insert_into_clauseContext) {
            LOGGER.trace { "enterInsert_into_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.BEGIN) {
                "Incorrect stage for parsing insert into clause, expected: ${Stage.BEGIN}, actual: $stage, text: ${ctx.text}"
            }
            stage = Stage.PARSING_NAMES
        }

        override fun enterColumn_name(ctx: PlSqlParser.Column_nameContext) {
            LOGGER.trace { "enterColumn_name token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_NAMES) {
                "Incorrect stage for parsing column name, expected: ${Stage.PARSING_NAMES}, actual: $stage, text: ${ctx.text}"
            }

            columNames.add(ctx.text.removeSurrounding("\""))
        }

        override fun exitInsert_into_clause(ctx: PlSqlParser.Insert_into_clauseContext) {
            LOGGER.trace { "exitInsert_into_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_NAMES) {
                "Incorrect stage for completing insert into clause, expected: ${Stage.PARSING_NAMES}, actual: $stage, text: ${ctx.text}"
            }
            stage = Stage.PARSED_NAMES
        }

        override fun enterValues_clause(ctx: PlSqlParser.Values_clauseContext) {
            LOGGER.trace { "enterValues_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSED_NAMES) {
                "Incorrect stage for parsing values clause, expected: ${Stage.PARSED_NAMES}, actual: $stage, text: ${ctx.text}"
            }
            stage = Stage.PARSING_VALUES
        }

        override fun enterUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
            LOGGER.trace { "enterUnary_expression token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing values clause, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }
            expressionHolder.add(UnaryExpression(ctx.start.tokenIndex))
        }

        override fun enterConstant(ctx: PlSqlParser.ConstantContext) {
            LOGGER.trace { "enterConstant token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing values clause, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            when {
                ctx.NULL_() != null -> expressionHolder.last().apply(null)
                ctx.numeric() != null -> {
                    expressionHolder.last().apply(
                        when {
                            ctx.numeric().UNSIGNED_INTEGER() != null -> ctx.text.toLong()
                            else -> ctx.text.toDouble()
                        }
                    )
                }
                ctx.quoted_string().isNotEmpty() -> { /* do nothing because this case is handled separate */ }
                else -> expressionHolder.last().apply(ctx.text)
            }
        }

        override fun enterQuoted_string(ctx: PlSqlParser.Quoted_stringContext) {
            LOGGER.trace { "enterQuoted_string token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing values clause, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            expressionHolder.last().apply(ctx.text.removeSurrounding("'"))
        }

        override fun enterOther_function(ctx: PlSqlParser.Other_functionContext) {
            LOGGER.trace { "enterOther_function token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing other function, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            when {
                ctx.TO_TIMESTAMP() != null -> expressionHolder.add(FunctionExpression("TO_TIMESTAMP", ctx.start.tokenIndex))
                else -> error("Unsupported other function ${ctx.text}")
            }
        }

        override fun exitOther_function(ctx: PlSqlParser.Other_functionContext) {
            LOGGER.trace { "exitOther_function token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing other function, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            exitFunction(ctx, "other function")
        }

        override fun enterString_function(ctx: PlSqlParser.String_functionContext) {
            LOGGER.trace { "enterString_function token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing string function, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            when {
                ctx.TO_DATE() != null -> expressionHolder.add(FunctionExpression("TO_DATE", ctx.start.tokenIndex))
                else -> error("Unsupported string function ${ctx.text}")
            }
        }

        override fun exitString_function(ctx: PlSqlParser.String_functionContext) {
            LOGGER.trace { "exitString_function token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing string function, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }

            exitFunction(ctx, "string function")
        }

        private fun exitFunction(ctx: ParserRuleContext, expressionName: String) {
            check(expressionHolder.isNotEmpty()) {
                "Expression with ${ctx.start.tokenIndex} isn't found for ${ctx.text} text"
            }
            val expression = expressionHolder.removeLast()
            check(expression.tokenIndex == ctx.start.tokenIndex) {
                "Internal problem during parse $expressionName expression ${ctx.text}"
            }
            expression.complete()
            expressionHolder.last().apply(expression.value)
        }

        override fun exitUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
            LOGGER.trace { "exitUnary_expression token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for parsing unary expression, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }
            val subExpressions: List<Expression> = expressionHolder.removeLastWhile {
                !(it.tokenIndex == ctx.start.tokenIndex && it is UnaryExpression)
            }

            check(expressionHolder.isNotEmpty()) {
                "Expression with ${ctx.start.tokenIndex} isn't found, text: ${ctx.text}"
            }
            val expression = expressionHolder.removeLast()
            check(expression.tokenIndex == ctx.start.tokenIndex) {
                "Internal problem during parse unary expression ${ctx.text}"
            }

            runCatching {
                when (subExpressions.size) {
                    0 -> {
                        expression.complete()
                    }

                    1 -> {
                        expression.apply(subExpressions.single().value)
                        expression.complete()
                    }

                    else -> error(
                        "Expression ${ctx.start.tokenIndex} must contain only one value instead of $subExpressions"
                    )
                }
            }.getOrElse {
                throw IllegalStateException("Unary expression can't be completed, text: ${ctx.text}", it)
            }

            when(expressionHolder.size) {
                0 -> {
                    val value = expression.value
                    val column = requireNotNull(columNames.poll()) {
                        "Column name isn't specified for $value value"
                    }
                    if (value != null) {
                        builder.put("${prefix}${column}", value)
                    }
                    expressionHolder.clear()
                    LOGGER.trace { "Handled '$column' to '$value' pair" }
                }
                else -> expressionHolder.last().apply(expression.value)
            }
        }

        override fun exitValues_clause(ctx: PlSqlParser.Values_clauseContext) {
            LOGGER.trace { "exitValues_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSING_VALUES) {
                "Incorrect stage for completing values clause, expected: ${Stage.PARSING_VALUES}, actual: $stage, text: ${ctx.text}"
            }
            stage = Stage.PARSED_VALUES
        }

        override fun enterMulti_table_insert(ctx: PlSqlParser.Multi_table_insertContext) {
            LOGGER.trace { "enterMulti_table_insert token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            error("Unsupported multiple table insert ${ctx.text}")
        }

        override fun exitInsert_statement(ctx: PlSqlParser.Insert_statementContext) {
            LOGGER.trace { "exitInsert_statement token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(stage == Stage.PARSED_VALUES) {
                "Incorrect stage for parsing insert statement, expected: ${Stage.PARSED_VALUES}, actual: $stage, text: ${ctx.text}"
            }
            check(columNames.isEmpty()) {
                "Incorrect stage for parsing insert statement, unused column names: $columNames, text: ${ctx.text}"
            }
            check(expressionHolder.isEmpty()) {
                "Incorrect stage for parsing insert statement, uncompleted values: $expressionHolder, text: ${ctx.text}"
            }
        }

        private enum class Stage {
            BEGIN,
            PARSING_NAMES,
            PARSED_NAMES,
            PARSING_VALUES,
            PARSED_VALUES,
        }
    }

    class UpdateListener(private val builder: MapBuilder<String, Any?>, private val prefix: String) : PlSqlParserBaseListener() {
        private val expressionHolder: MutableList<Expression> = mutableListOf()
        private var rowValue: RowValue? = null

        override fun visitErrorNode(node: ErrorNode) {
            super.visitErrorNode(node)
        }

        override fun enterUpdate_statement(ctx: PlSqlParser.Update_statementContext) {
            LOGGER.trace { "enterUpdate_statement token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }

            expressionHolder.clear()
        }

        override fun enterColumn_based_update_set_clause(ctx: PlSqlParser.Column_based_update_set_clauseContext) {
            LOGGER.trace { "enterColumn_based_update_set_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(rowValue == null) {
                "Incorrect state for parsing column name: previous row value isn't null $rowValue, text: ${ctx.text}"
            }
            rowValue = RowValue(ctx.start.tokenIndex)
        }

        override fun enterColumn_name(ctx: PlSqlParser.Column_nameContext) {
            LOGGER.trace { "enterColumn_name token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            requireNotNull(rowValue) {
                "Incorrect state for parsing column name: previous row value isn't null $rowValue, text: ${ctx.text}"
            }.apply {
                column = ctx.text.removeSurrounding("\"")
            }
        }

        override fun enterUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
            if (!checkStage(ctx, "enterUnary_expression")) return

            expressionHolder.add(UnaryExpression(ctx.start.tokenIndex))
        }

        override fun enterConstant(ctx: PlSqlParser.ConstantContext) {
            if (!checkStage(ctx, "enterConstant")) return

            when {
                ctx.NULL_() != null -> expressionHolder.last().apply(null)
                ctx.numeric() != null -> {
                    expressionHolder.last().apply(
                        when {
                            ctx.numeric().UNSIGNED_INTEGER() != null -> ctx.text.toLong()
                            else -> ctx.text.toDouble()
                        }
                    )
                }
                ctx.quoted_string().isNotEmpty() -> { /* do nothing because this case is handled separate */ }
                else -> expressionHolder.last().apply(ctx.text)
            }
        }

        override fun enterQuoted_string(ctx: PlSqlParser.Quoted_stringContext) {
            if (!checkStage(ctx, "enterQuoted_string")) return

            expressionHolder.last().apply(ctx.text.removeSurrounding("'"))
        }

        override fun enterOther_function(ctx: PlSqlParser.Other_functionContext) {
            if (!checkStage(ctx, "enterOther_function")) return

            when {
                ctx.TO_TIMESTAMP() != null -> expressionHolder.add(FunctionExpression("TO_TIMESTAMP", ctx.start.tokenIndex))
                else -> error("Unsupported other function ${ctx.text}")
            }
        }

        override fun exitOther_function(ctx: PlSqlParser.Other_functionContext) {
            if (!checkStage(ctx, "exitOther_function")) return

            exitFunction(ctx, "other function")
        }

        override fun enterString_function(ctx: PlSqlParser.String_functionContext) {
            if (!checkStage(ctx, "enterString_function")) return

            when {
                ctx.TO_DATE() != null -> expressionHolder.add(FunctionExpression("TO_DATE", ctx.start.tokenIndex))
                else -> error("Unsupported string function ${ctx.text}")
            }
        }

        override fun exitString_function(ctx: PlSqlParser.String_functionContext) {
            if (!checkStage(ctx, "exitString_function")) return

            exitFunction(ctx, "string function")
        }

        private fun exitFunction(ctx: ParserRuleContext, expressionName: String) {
            check(expressionHolder.isNotEmpty()) {
                "Expression with ${ctx.start.tokenIndex} isn't found for ${ctx.text} text"
            }
            val expression = expressionHolder.removeLast()
            check(expression.tokenIndex == ctx.start.tokenIndex) {
                "Internal problem during parse $expressionName expression ${ctx.text}"
            }
            expression.complete()
            expressionHolder.last().apply(expression.value)
        }

        override fun exitUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
            if (!checkStage(ctx, "exitUnary_expression")) return

            val subExpressions: List<Expression> = expressionHolder.removeLastWhile {
                !(it.tokenIndex == ctx.start.tokenIndex && it is UnaryExpression)
            }

            check(expressionHolder.isNotEmpty()) {
                "Expression with ${ctx.start.tokenIndex} isn't found, text: ${ctx.text}"
            }
            val expression = expressionHolder.removeLast()
            check(expression.tokenIndex == ctx.start.tokenIndex) {
                "Internal problem during parse unary expression ${ctx.text}"
            }

            runCatching {
                when (subExpressions.size) {
                    0 -> {
                        expression.complete()
                    }

                    1 -> {
                        expression.apply(subExpressions.single().value)
                        expression.complete()
                    }

                    else -> error(
                        "Expression ${ctx.start.tokenIndex} must contain only one value instead of $subExpressions"
                    )
                }
            }.getOrElse {
                throw IllegalStateException("Unary expression can't be completed, text: ${ctx.text}", it)
            }

            when(expressionHolder.size) {
                0 -> {
                    rowValue?.value = expression.value
                    expressionHolder.clear()
                }
                else -> expressionHolder.last().apply(expression.value)
            }
        }

        override fun exitColumn_based_update_set_clause(ctx: PlSqlParser.Column_based_update_set_clauseContext) {
            LOGGER.trace { "enterColumn_based_update_set_clause token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            requireNotNull(rowValue) {
                "Incorrect state for parsing column name: current row value is null, text: ${ctx.text}"
            }.apply {
                check(isCompleted) {
                    "Incorrect state for parsing column name: current row value isn't completed $this, text: ${ctx.text}"
                }

                builder.put("${prefix}${column}", value)
                rowValue = null
                LOGGER.trace { "Handled '$column' to '$value' pair" }
            }

        }

        override fun exitUpdate_statement(ctx: PlSqlParser.Update_statementContext) {
            LOGGER.trace { "exitUpdate_statement token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            check(rowValue == null) {
                "Incorrect stage for parsing update statement, uncompleted row value pair: $rowValue, text: ${ctx.text}"
            }
            check(expressionHolder.isEmpty()) {
                "Incorrect stage for parsing update statement, uncompleted values: $expressionHolder, text: ${ctx.text}"
            }
        }

        /**
         * Use this check to verify stage of parsing
         * SET and WHERE blocks uses the same tokens
         */
        private fun checkStage(ctx: ParserRuleContext, methodName: String): Boolean {
            if (rowValue == null) {
                LOGGER.trace { "$methodName token index: ${ctx.start.tokenIndex}, text: ${ctx.text}, SKIPPED" }
                return false
            }
            LOGGER.trace { "$methodName token index: ${ctx.start.tokenIndex}, text: ${ctx.text}" }
            return true
        }
    }

    companion object {
        private val LOGGER = KotlinLogging.logger {}
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

        private inline fun <T> MutableList<T>.removeLastWhile(predicate: (T) -> Boolean): List<T> {
            if (isEmpty())
                return emptyList()
            val iterator = listIterator(size)
            while (iterator.hasPrevious()) {
                if (!predicate(iterator.previous())) {
                    iterator.next()
                    val expectedSize = size - iterator.nextIndex()
                    if (expectedSize == 0) return emptyList()
                    return ArrayList<T>(expectedSize).apply {
                        while (iterator.hasNext()) {
                            add(iterator.next())
                            iterator.remove()
                        }
                    }
                }
            }
            return toList()
        }

        private interface Expression {
            val value: Any?
            val tokenIndex: Int
            val isCompleted: Boolean

            fun apply(value: Any?)
            fun complete()
        }

        private class UnaryExpression(
            override val tokenIndex: Int,
        ) : Expression {
            override val value: Any?
                get() = _value.also {
                    check(isCompleted) {
                        "${javaClass.simpleName} $tokenIndex isn't completed"
                    }
                }
            private var _value: Any? = null
            override var isCompleted = false

            override fun apply(value: Any?) {
                check(!isCompleted) {
                    "${javaClass.simpleName} expression $tokenIndex can't be completed twice, value for complete: $value, actual value: $_value"
                }
                _value = value
                isCompleted = true
            }

            override fun complete() {
                check(isCompleted) {
                    "${javaClass.simpleName} expression $tokenIndex isn't completed"
                }
            }

            override fun toString(): String {
                return "${javaClass.simpleName}(tokenIndex=$tokenIndex, _value=$_value, isCompleted=$isCompleted)"
            }
        }

        private class FunctionExpression(
            funcName: String,
            override val tokenIndex: Int,
        ) : Expression {
            override val value: Any
                get() = _value.also {
                    check(isCompleted) {
                        "${javaClass.simpleName} $tokenIndex isn't completed"
                    }
                }

            override var isCompleted = false

            private val _value: MutableMap<String, Any?> = mutableMapOf(
                "function" to funcName
            )
            private val list: MutableList<Any?> by lazy { mutableListOf() }

            override fun apply(value: Any?) {
                check(!isCompleted) {
                    "${javaClass.simpleName} $tokenIndex is already completed, value: $value, actual value: $_value"
                }

                list.add(value)
                _value.putIfAbsent("parameters", list)
            }

            override fun complete() {
                isCompleted = true
            }

            override fun toString(): String {
                return "${javaClass.simpleName}(tokenIndex=$tokenIndex, _value=$_value, list=$list, isCompleted=$isCompleted)"
            }
        }

        private class RowValue(
            private val tokenIndex: Int,
        ) {
            private var _column: String = ""
            private var _value: Any? = null

            private var isColumnSet = false
            private var isValueSet = false

            var column: String
                get() {
                    check(isCompleted) {
                        "${javaClass.simpleName} $tokenIndex isn't completed"
                    }
                    return _column
                }
                set(value) {
                    check(!isColumnSet) {
                        "${javaClass.simpleName} $tokenIndex already has column, suggested: $value, actual: $_column"
                    }
                    _column = value
                    isColumnSet = true
                }
            var value: Any?
                get() = _value.also {
                    check(isCompleted) {
                        "${javaClass.simpleName} $tokenIndex isn't completed"
                    }
                }
                set(value) {
                    check(!isValueSet) {
                        "${javaClass.simpleName} $tokenIndex already has value, suggested: $value, actual: $_value"
                    }
                    _value = value
                    isValueSet = true
                }
            val isCompleted: Boolean
                get() = isColumnSet && isValueSet

            override fun toString(): String {
                return "RowValue(tokenIndex=$tokenIndex, _column='$_column', _value=$_value, isValueSet=$isValueSet)"
            }
        }
    }
}
