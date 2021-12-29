package com.heliorm.sql;

import java.sql.JDBCType;

public class SqlIntegerColumn extends SqlColumn implements IntegerColumn {

    SqlIntegerColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement) {
        super(table, name, jdbcType, nullable, autoIncrement);
    }

}
