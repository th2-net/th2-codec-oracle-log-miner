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

internal class RowValue(
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