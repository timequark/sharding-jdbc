package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import com.dangdang.ddframe.rdb.sharding.parsing.parser.expression.SQLExpression;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class ConditionItem {
    private final EComparisonType comparisonType;
    private final SQLExpression leftExpress;
    private final SQLExpression rightExpress;
}
