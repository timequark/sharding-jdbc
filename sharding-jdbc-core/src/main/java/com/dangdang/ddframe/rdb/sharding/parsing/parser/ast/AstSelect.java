package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.constant.SQLType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.OrderItem;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.limit.Limit;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.selectitem.SelectItem;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.context.table.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
public final class AstSelect extends AbstractAst {
    /**
     * 是否查询所有字段，即 SELECT *
     * 单独加了这个字段的标志原因是，一些业务地方会判断是否需要的字段已经查询，例如 GROUP BY / ORDER BY
     */
    private boolean containStar;
    /**
     * 查询项
     */
    private final List<SelectItem> items = new LinkedList<>();

    /**
     * Where条件组
     */
    private ConditionGroup conditionGroup;

    /**
     * 分组项
     */
    private final List<OrderItem> groupByItems = new LinkedList<>();
    /**
     * 排序项
     */
    private final List<OrderItem> orderByItems = new LinkedList<>();
    /**
     * 分页
     */
    private Limit limit;

    public AstSelect(SQLType sqlType) {
        super(sqlType);
    }
}
