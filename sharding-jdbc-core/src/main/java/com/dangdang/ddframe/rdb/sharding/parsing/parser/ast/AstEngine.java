package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.DefaultKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.dialect.mysql.MySQLParser;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.model.AstSelect;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.exception.SQLParsingUnsupportedException;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class AstEngine {
    /**
     * 数据库类型
     */
    private final DatabaseType dbType;
    /**
     * SQL
     */
    private final String sql;
    /**
     * 分片规则
     */

    private SQLParser getSQLParser() {
        switch (dbType) {
            case H2:
            case MySQL:
                return new MySQLParser(sql);
            case Oracle:
            case SQLServer:
            case PostgreSQL:
            default:
                throw new UnsupportedOperationException(dbType.name());
        }
    }

    public AstSelect parse() {
        // 获取 SQL解析器
        SQLParser sqlParser = getSQLParser();

        if (sqlParser.equalAny(DefaultKeyword.SELECT)) {
            return AstSelectParserFactory.newInstance(sqlParser).parse();
        }
        throw new SQLParsingUnsupportedException(sqlParser.getLexer().getCurrentToken().getType());
    }
}
