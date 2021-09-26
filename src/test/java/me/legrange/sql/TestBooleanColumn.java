package me.legrange.sql;

import java.sql.JDBCType;

public class TestBooleanColumn extends TestColumn implements BooleanColumn {
    public TestBooleanColumn(Table table, String name) {
        super(table, name, JDBCType.BOOLEAN);
    }
}
