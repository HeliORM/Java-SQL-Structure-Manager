package com.heliorm.sql;

import java.sql.JDBCType;

public class SqlTimeStampColumn extends SqlColumn implements TimeStampColumn{
    SqlTimeStampColumn(Table table, String name, JDBCType jdbcType, boolean nullable, String defaultValue) {
        super(table, name, jdbcType, nullable, defaultValue, false);
    }
}
