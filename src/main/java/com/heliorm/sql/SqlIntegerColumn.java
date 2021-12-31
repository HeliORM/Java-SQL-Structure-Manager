package com.heliorm.sql;

import java.sql.JDBCType;

/** An implementation of an integer column read from SQL */
class SqlIntegerColumn extends SqlColumn implements IntegerColumn {

    SqlIntegerColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement) {
        super(table, name, jdbcType, nullable, autoIncrement);
    }

}
