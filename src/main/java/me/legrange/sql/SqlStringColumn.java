package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;

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