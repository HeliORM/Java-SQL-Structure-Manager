package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Collections;

public class TestIntegerColumn extends TestColumn implements IntegerColumn{

    public TestIntegerColumn(Table table, String name, JDBCType jdbcType) {
        super(table, name, jdbcType);
    }

    public TestIntegerColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean key, boolean autoIncrement) {
        super(table, name, jdbcType, nullable,  null, key, autoIncrement, Collections.emptySet());
    }

    public TestIntegerColumn(Table table, String name, JDBCType jdbcType, boolean nullable, String defaultValue, boolean key) {
        super(table, name, jdbcType, nullable,  defaultValue, key, false, Collections.emptySet());
    }

}
