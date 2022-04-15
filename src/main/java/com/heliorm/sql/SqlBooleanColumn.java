package com.heliorm.sql;

import java.sql.JDBCType;

/**
 * Implementation of boolean column that is populated by reading from SQL
 */
final class SqlBooleanColumn extends SqlColumn implements BooleanColumn {

    public SqlBooleanColumn(Table table, String name, boolean nullable, String defVal) {
        super(table, name, JDBCType.BOOLEAN, nullable, defVal, false);
    }
}
