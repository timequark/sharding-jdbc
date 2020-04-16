package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.dialect.mysql;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.dialect.mysql.MySQLLexer;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.AstParser;

/**
 * MySQL AST 解析器，类似 MySQLParser
 *
 * @author liuhao
 */
public class MySQLAstParser extends AstParser {
    public MySQLAstParser(final String sql) {
        super(new MySQLLexer(sql));
    }
}
