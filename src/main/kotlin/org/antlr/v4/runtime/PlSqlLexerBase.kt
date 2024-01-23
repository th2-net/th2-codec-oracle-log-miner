package org.antlr.v4.runtime

abstract class PlSqlLexerBase(input: CharStream?) : Lexer(input) {
    var self: PlSqlLexerBase = this

    protected fun IsNewlineAtPos(pos: Int): Boolean {
        val la = _input.LA(pos)
        return la == -1 || la == '\n'.code
    }
}
