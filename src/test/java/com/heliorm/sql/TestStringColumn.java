package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Collections;

public class TestStringColumn extends TestColumn implements StringColumn {

    private final int length;

    public TestStringColumn(Table table, String name, JDBCType jdbcType, int length) {
        super(table, name, jdbcType);
        this.length = length;
    }

    public TestStringColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean key, int length) {
        super(table, name, jdbcType, nullable, key, false);
        this.length = length;
    }

    public TestStringColumn(Table table, String name, JDBCType jdbcType, boolean nullable, String defaultValue, boolean key, boolean autoIncrement, int length) {
        super(table, name, jdbcType, nullable, defaultValue, key, autoIncrement, Collections.emptySet());
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }
}
