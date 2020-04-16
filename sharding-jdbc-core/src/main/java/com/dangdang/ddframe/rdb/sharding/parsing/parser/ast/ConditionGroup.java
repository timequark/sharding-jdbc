package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

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
    private final List<ConditionItem> items = new LinkedList<>();
    private ConditionGroup next;
}
