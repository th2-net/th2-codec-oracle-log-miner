package com.exactpro.th2.codec.oracle.logminer.antlr

import org.antlr.v4.runtime.Parser
import org.antlr.v4.runtime.TokenStream

abstract class PlSqlParserBase(input: TokenStream?) : Parser(input) {
    var isVersion12: Boolean = true
    var isVersion10: Boolean = true
    var self: PlSqlParserBase = this
}
