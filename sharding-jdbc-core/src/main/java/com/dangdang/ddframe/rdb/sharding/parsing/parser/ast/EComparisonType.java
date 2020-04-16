package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

@RequiredArgsConstructor
@Getter
public enum EComparisonType {
    EQ("="),
    GT(">"),
    LT("<"),
    LT_EQ("<="),
    GT_EQ(">="),
    LT_GT("<>"),
    BANG_EQ("!="),

    IN("IN"),
    BETWEEN("BETWEEN"),
    LIKE("LIKE");

    private static Map<String, EComparisonType> symbols = new HashMap<>(128);

    static {
        for (EComparisonType each : EComparisonType.values()) {
            symbols.put(each.getLiterals(), each);
        }
    }

    private final String literals;

    /**
     * 通过字面量查找比较运算符.
     *
     * @param literals 字面量
     * @return 词法符号
     */
    public static EComparisonType literalsOf(final String literals) {
        return symbols.get(literals.toUpperCase());
    }
}
