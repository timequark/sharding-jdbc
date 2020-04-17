package com.dangdang.ddframe.rdb.sharding.parsing.parser.ast.model;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Token;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
@Setter
@Getter
public class TokenNode {
    private TokenNode left;
    private TokenNode right;
    private Token value;
}
