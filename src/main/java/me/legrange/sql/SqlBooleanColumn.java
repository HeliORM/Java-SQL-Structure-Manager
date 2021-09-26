package me.legrange.sql;

import java.sql.JDBCType;

final class SqlBooleanColumn extends SqlColumn implements BooleanColumn {

    public SqlBooleanColumn(Table table, String name,  boolean nullable) {
        super(table, name, JDBCType.BOOLEAN, nullable, false);
    }
}
