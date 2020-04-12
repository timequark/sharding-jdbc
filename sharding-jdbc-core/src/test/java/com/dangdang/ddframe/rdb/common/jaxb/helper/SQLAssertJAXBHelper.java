package com.dangdang.ddframe.rdb.common.jaxb.helper;

import com.dangdang.ddframe.rdb.common.jaxb.SQLAssert;
import com.dangdang.ddframe.rdb.common.jaxb.SQLAsserts;
import com.dangdang.ddframe.rdb.sharding.constant.DatabaseType;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class SQLAssertJAXBHelper {
    
    public static Collection<Object[]> getDataParameters(final String filePath) {
        Collection<Object[]> result = new ArrayList<>();
        URL url = SQLAssertJAXBHelper.class.getClassLoader().getResource(filePath);
        if (null == url) {
            return Collections.emptyList();
        }
        File assertFilePath = new File(url.getPath());
        if (!assertFilePath.exists()) {
            return Collections.emptyList();
        }
        
        if (assertFilePath.isDirectory()) {
            File[] files = assertFilePath.listFiles();
            if (null == files) {
                return Collections.emptyList();
            }
            for (File each : files) {
                if (each.isDirectory()) {
                    continue;
                }
                result.addAll(dataParameters(each));
            }
        } else {
            result.addAll(dataParameters(assertFilePath));
        }
        
        return result;
    }
    
    private static Collection<Object[]> dataParameters(final File file) {
        SQLAsserts asserts = loadSqlAsserts(file);
        Object[][] result = new Object[asserts.getSqlAsserts().size()][1];
        for (int i = 0; i < asserts.getSqlAsserts().size(); i++) {
            result[i] = getDataParameter(asserts.getSqlAsserts().get(i));
        }
        return Arrays.asList(result);
    }
    
    private static SQLAsserts loadSqlAsserts(final File file) {
        try {
            return (SQLAsserts) JAXBContext.newInstance(SQLAsserts.class).createUnmarshaller().unmarshal(file);
        } catch (final JAXBException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private static Object[] getDataParameter(final SQLAssert sqlAssert) {
        final Object[] result = new Object[4];
        result[0] = sqlAssert.getId();
        result[1] = sqlAssert.getSql();
        if (null == sqlAssert.getTypes()) {
            result[2] = Collections.emptySet();
        } else {
            Set<DatabaseType> types = new HashSet<>();
            for (String each : sqlAssert.getTypes().split(",")) {
                types.add(DatabaseType.valueOf(each));
            }
            result[2] = types;
        }
        result[3] = sqlAssert.getSqlShardingRules();
        return result;
    }
}
