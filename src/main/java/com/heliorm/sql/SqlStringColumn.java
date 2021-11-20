package com.heliorm.sql;

import java.sql.JDBCType;


/** Implementation of string column that is populated by reading from SQL */
class SqlStringColumn extends SqlColumn implements StringColumn {

    private final int length;

    public SqlStringColumn(Table table, String name, JDBCType jdbcType, boolean nullable, int length) {
        super(table, name, jdbcType, nullable, false);
        this.length = length;
    }

    @Override
    public int getLength() {
        return length;
    }
}
