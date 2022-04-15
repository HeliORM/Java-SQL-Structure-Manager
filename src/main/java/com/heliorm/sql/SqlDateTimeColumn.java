package com.heliorm.sql;

import java.sql.JDBCType;

public class SqlDateTimeColumn extends SqlColumn {

    SqlDateTimeColumn(Table table, String name, JDBCType jdbcType, boolean nullable) {
        super(table, name, jdbcType, nullable,null,false);
    }
}
