package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Set;

public class TestIntegerColumn extends TestColumn implements IntegerColumn{
    public TestIntegerColumn(Table table, String name, JDBCType jdbcType) {
        super(table, name, jdbcType);
    }

    public TestIntegerColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean key, boolean autoIncrement) {

        super(table, name, jdbcType, nullable, key, autoIncrement);
    }

}
