package com.heliorm.sql;

import java.sql.JDBCType;

public class TestBinaryColumn extends TestColumn implements BinaryColumn {

    private final int length;
    public TestBinaryColumn(Table table, String name, JDBCType jdbcType, int length) {
        super(table, name, jdbcType);
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }
}
