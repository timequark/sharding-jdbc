package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.cond;

import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.rel.EComparisonType;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.expression.SQLExpression;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
@Getter
public class ConditionItem {
    private final EComparisonType comparisonType;
    private final SQLExpression leftExpr;
    private final List<SQLExpression> rightExprs = new LinkedList<>();
}
