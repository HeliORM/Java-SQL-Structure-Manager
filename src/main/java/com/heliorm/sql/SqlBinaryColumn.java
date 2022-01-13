package com.heliorm.sql;

import java.sql.JDBCType;

public class SqlBinaryColumn extends SqlColumn implements BinaryColumn {

    private final int length;

    SqlBinaryColumn(Table table, String name, JDBCType jdbcType, boolean nullable, int length) {
        super(table, name, jdbcType, nullable, false);
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }
}