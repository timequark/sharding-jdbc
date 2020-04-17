package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.model;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.table.Table;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Setter
@Getter
public abstract class Ast {
    /**
     * SQL 类型
     */
    private final SQLType type;

    /**
     * 表
     */
    protected Table table;
}
