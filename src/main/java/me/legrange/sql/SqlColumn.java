package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;
import java.util.Set;

class SqlColumn implements Column {

    private final Table table;
    private final String name;
    private final JDBCType jdbcType;
    private final boolean nullable;
    private boolean key;
    private boolean autoIncrement;

    SqlColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement) {
        this.table = table;
        this.name = name;
        this.jdbcType = jdbcType;
        this.nullable = nullable;
        this.key = false;
        this.autoIncrement = autoIncrement;
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
    public String toString() {
        return "SqlColumn{" +
                "table=" + table +
                ", name='" + name + '\'' +
                ", jdbcType=" + jdbcType +
                ", nullable=" + nullable +
                ", key=" + key +
                ", autoIncrement=" + autoIncrement +
                '}';
    }


}
