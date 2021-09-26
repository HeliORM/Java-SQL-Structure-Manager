package me.legrange.sql;

import org.junit.jupiter.api.Test;

import java.sql.JDBCType;

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

    @Override
    public int getLength() {
        return length;
    }
}
