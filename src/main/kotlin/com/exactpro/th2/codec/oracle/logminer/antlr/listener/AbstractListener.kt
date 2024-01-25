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
import com.exactpro.th2.codec.oracle.logminer.antlr.PlSqlParserBaseListener
import org.antlr.v4.runtime.ParserRuleContext

abstract class AbstractListener: PlSqlParserBaseListener() {

    internal val expressionHolder: MutableList<Expression> = mutableListOf()

    override fun enterUnary_expression(ctx: PlSqlParser.Unary_expressionContext) {
        expressionHolder.add(UnaryExpression(ctx.start.tokenIndex))
    }

    override fun enterConstant(ctx: PlSqlParser.ConstantContext) {
        when {
            ctx.NULL_() != null -> expressionHolder.last().appendValue(null)
            ctx.numeric() != null -> {
                expressionHolder.last().appendValue(
                    when {
                        ctx.numeric().UNSIGNED_INTEGER() != null -> ctx.text.toLong()
                        else -> ctx.text.toDouble()
                    }
                )
            }
            ctx.quoted_string().isNotEmpty() -> { /* do nothing because this case is handled separate */ }
            else -> expressionHolder.last().appendValue(ctx.text)
        }
    }

    override fun enterQuoted_string(ctx: PlSqlParser.Quoted_stringContext) {
        expressionHolder.last().appendValue(ctx.text.removeSurrounding("'"))
    }

    override fun exitOther_function(ctx: PlSqlParser.Other_functionContext) {
        exitFunction(ctx, "other function")
    }

    override fun enterOther_function(ctx: PlSqlParser.Other_functionContext) {
        when {
            ctx.TO_TIMESTAMP() != null -> expressionHolder.add(
                FunctionExpression(
                    "TO_TIMESTAMP",
                    ctx.start.tokenIndex
                )
            )
            else -> error("Unsupported other function ${ctx.text}")
        }
    }

    override fun enterString_function(ctx: PlSqlParser.String_functionContext) {
        when {
            ctx.TO_DATE() != null -> expressionHolder.add(
                FunctionExpression(
                    "TO_DATE",
                    ctx.start.tokenIndex
                )
            )
            else -> error("Unsupported string function ${ctx.text}")
        }
    }

    override fun exitString_function(ctx: PlSqlParser.String_functionContext) {
        exitFunction(ctx, "string function")
    }

    internal fun handle(ctx: PlSqlParser.Unary_expressionContext): Expression {
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
                    expression.appendValue(subExpressions.single().value)
                    expression.complete()
                }

                else -> error(
                    "Expression ${ctx.start.tokenIndex} must contain only one value instead of $subExpressions"
                )
            }
        }.getOrElse {
            throw IllegalStateException("Unary expression can't be completed, text: ${ctx.text}", it)
        }
        return expression
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
        expressionHolder.last().appendValue(expression.value)
    }

    companion object {
        @JvmStatic
        protected inline fun <T> MutableList<T>.removeLastWhile(predicate: (T) -> Boolean): List<T> {
            if (isEmpty())
                return emptyList()
            val iterator = listIterator(size)
            while (iterator.hasPrevious()) {
                if (!predicate(iterator.previous())) {
                    iterator.next()
                    val expectedSize = size - iterator.nextIndex()
                    if (expectedSize == 0) return emptyList()
                    return buildList(expectedSize) {
                        while (iterator.hasNext()) {
                            add(iterator.next())
                            iterator.remove()
                        }
                    }
                }
            }
            return this
        }
    }
}