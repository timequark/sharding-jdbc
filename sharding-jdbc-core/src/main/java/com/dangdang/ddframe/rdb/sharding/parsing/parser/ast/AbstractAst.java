package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.table.Table;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public abstract class AbstractAst {
    /**
     * SQL 类型
     */
    private final SQLType type;

    /**
     * 表
     */
    private final Table table;
}
