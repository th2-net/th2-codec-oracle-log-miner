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

internal class FunctionExpression(
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

    override fun appendValue(value: Any?) {
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