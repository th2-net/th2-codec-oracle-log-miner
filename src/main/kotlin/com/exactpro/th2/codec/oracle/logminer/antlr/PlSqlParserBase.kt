package com.exactpro.th2.codec.oracle.logminer.antlr

import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.TokenStream

abstract class PlSqlParserBase(input: TokenStream?) : Parser(input) {
    val isVersion12: Boolean = true
    val isVersion10: Boolean = true
    val self: PlSqlParserBase
        get() = this
}
