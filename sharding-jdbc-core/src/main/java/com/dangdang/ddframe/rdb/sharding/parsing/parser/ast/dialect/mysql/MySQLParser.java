package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.dialect.mysql;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.dialect.mysql.MySQLLexer;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.SQLParser;

/**
 * 等同 com.dangdang.ddframe.rdb.sharding.parsing.parser.dialect.mysql.MySQLParser
 *
 * @author liuhao
 */
public class MySQLParser extends SQLParser {
    public MySQLParser(final String sql) {
        super(new MySQLLexer(sql));
    }
}
