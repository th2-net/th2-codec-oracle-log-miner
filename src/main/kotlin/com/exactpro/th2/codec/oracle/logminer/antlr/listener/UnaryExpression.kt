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

internal class UnaryExpression(
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

    override fun appendValue(value: Any?) {
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