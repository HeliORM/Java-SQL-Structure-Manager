package com.heliorm.sql;

import java.sql.JDBCType;


/** Implementation of a column that is populated by reading from SQL
 *
 */
abstract class SqlColumn implements Column {

    private final Table table;
    private final String name;
    private final JDBCType jdbcType;
    private final boolean nullable;
    private boolean key;
    private boolean autoIncrement;
    private String defaultValue;

    SqlColumn(Table table, String name, JDBCType jdbcType, boolean nullable, String defaultValue, boolean autoIncrement) {
        this.table = table;
        this.name = name;
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.key = false;
        this.autoIncrement = autoIncrement;
        this.defaultValue = defaultValue;
    }

    void setKey(boolean key) {
        this.key = key;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public JDBCType getJdbcType() {
        return jdbcType;
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public boolean isKey() {
        return key;
    }

    @Override
    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    @Override
    public String getDefault() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return "SqlColumn{" +
                "autoIncrement=" + autoIncrement +
                ", defaultValue='" + defaultValue + '\'' +
                ", jdbcType=" + jdbcType +
                ", key=" + key +
                ", name='" + name + '\'' +
                ", nullable=" + nullable +
                ", table=" + table +
                '}';
    }
}
