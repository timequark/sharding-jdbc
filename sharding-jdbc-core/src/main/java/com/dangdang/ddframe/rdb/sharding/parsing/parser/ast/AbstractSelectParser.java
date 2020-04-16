package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.statement.SQLStatementParser;
import lombok.AccessLevel;
import lombok.Getter;

@Getter(AccessLevel.PROTECTED)
public abstract class AbstractSelectParser implements SQLStatementParser {

    private final AstParser sqlParser;

    private final AstSelect astSelect;

    public AbstractSelectParser(final AstParser sqlParser) {
        this.sqlParser = sqlParser;
        this.astSelect = new AstSelect(SQLType.DQL);
    }

}
