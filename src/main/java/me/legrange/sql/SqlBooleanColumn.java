package me.legrange.sql;

import java.sql.JDBCType;

final class SqlBooleanColumn extends SqlColumn implements BooleanColumn {

    public SqlBooleanColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement) {
        super(table, name, jdbcType, nullable, autoIncrement);
    }
}
