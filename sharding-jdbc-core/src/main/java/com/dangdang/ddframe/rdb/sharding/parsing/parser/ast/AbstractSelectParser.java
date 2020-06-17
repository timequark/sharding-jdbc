package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.model.AstSelect;
import lombok.AccessLevel;
import lombok.Getter;

@Getter(AccessLevel.PROTECTED)
public abstract class AbstractSelectParser implements AstParser {

    private final SQLParser sqlParser;

    private final AstSelect astSelect;

    public AbstractSelectParser(final SQLParser sqlParser) {
        this.sqlParser = sqlParser;
        this.astSelect = new AstSelect(SQLType.DQL);
    }

    protected abstract void query();

    @Override
    public final AstSelect parse() {
        query();

        // TODO

        return astSelect;
    }
}
