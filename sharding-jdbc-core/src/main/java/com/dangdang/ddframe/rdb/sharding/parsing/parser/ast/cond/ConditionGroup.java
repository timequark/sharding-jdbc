package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.cond;

import com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.rel.ELogicType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

@RequiredArgsConstructor
@Setter
@Getter
public class ConditionGroup {
    private final ELogicType logicType;
    private final ConditionItem left;
    private final ConditionGroup right;
}
