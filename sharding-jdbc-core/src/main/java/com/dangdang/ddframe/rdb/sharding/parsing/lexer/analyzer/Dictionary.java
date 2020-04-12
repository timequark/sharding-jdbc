/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.rdb.sharding.parsing.lexer.analyzer;

import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.DefaultKeyword;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.Keyword;
import com.dangdang.ddframe.rdb.sharding.parsing.lexer.token.TokenType;

import java.util.HashMap;
import java.util.Map;

/**
 * 词法标记字典.
 *
 * @author zhangliang
 */
public final class Dictionary {

    /**
     * 词法关键词Map
     */
    private final Map<String, Keyword> tokens = new HashMap<>(1024);
    
    public Dictionary(final Keyword... dialectKeywords) {
        fill(dialectKeywords);
    }

    /**
     * 装上默认词法关键词 + 方言词法关键词
     * 不同的数据库有相同的默认词法关键词，有有不同的方言关键词
     *
     * @param dialectKeywords 方言词法关键词
     */
    private void fill(final Keyword... dialectKeywords) {
        for (DefaultKeyword each : DefaultKeyword.values()) {
            tokens.put(each.name(), each);
        }
        for (Keyword each : dialectKeywords) {
            tokens.put(each.toString(), each);
        }
    }

    /**
     * 获得 词法字面量 对应的 词法字面量标记
     * 当不存在时，返回默认词法字面量标记
     *
     * @param literals 词法字面量
     * @param defaultTokenType 默认词法字面量标记
     * @return 词法字面量标记
     */
    TokenType findTokenType(final String literals, final TokenType defaultTokenType) {
        String key = null == literals ? null : literals.toUpperCase();
        return tokens.containsKey(key) ? tokens.get(key) : defaultTokenType; // TODO 直接get，然后判断是否为空
    }

    /**
     * 获得 词法字面量 对应的 词法字面量标记
     * 当不存在时，抛出 {@link IllegalArgumentException}
     *
     * @param literals 词法字面量
     * @return 词法字面量标记
     */
    TokenType findTokenType(final String literals) {
        String key = null == literals ? null : literals.toUpperCase();
        if (tokens.containsKey(key)) {
            return tokens.get(key);
        }
        throw new IllegalArgumentException();
    }
}
