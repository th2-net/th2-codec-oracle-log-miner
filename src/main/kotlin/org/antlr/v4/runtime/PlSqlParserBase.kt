package org.antlr.v4.runtime

abstract class PlSqlParserBase(input: TokenStream?) : Parser(input) {
    var isVersion12: Boolean = true
    var isVersion10: Boolean = true
    var self: PlSqlParserBase = this
}
