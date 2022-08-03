package com.heliorm.sql;

import java.sql.JDBCType;

public class SqlDateTimeColumn extends SqlColumn implements DateTimeColumn {

    SqlDateTimeColumn(Table table, String name, JDBCType jdbcType, boolean nullable, String defVal) {
        super(table, name, jdbcType, nullable, defVal, false);
    }
}
