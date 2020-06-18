package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.cond;

import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.rel.EConditionGroup;
import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.rel.ELogicType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Setter
@Getter
public class ConditionGroup {
    private final EConditionGroup conditionGroup;
    private final ConditionItem item;
    private final ELogicType logicType;
    private final ConditionGroup next;
}
