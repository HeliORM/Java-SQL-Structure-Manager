package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;

final class SqlColumn implements Column {

    private final Table table;
    private final String name;
    private final JDBCType jdbcType;
    private final Optional<Integer> length;
    private final boolean nullable;
    private boolean key;
    private boolean autoIncrement;

    SqlColumn(Table table, String name, JDBCType jdbcType, Optional<Integer> length, boolean nullable, boolean autoIncrement) {
        this.table = table;
        this.name = name;
        this.jdbcType = jdbcType;
        this.length = length;
        this.nullable = nullable;
        this.key = false;
        this.autoIncrement = autoIncrement;
    }

    void setKey(boolean key) {
        this.key = key;
    }

    void setAutoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
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
    public Optional<Integer> getLength() {
        return length;
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
                ", length=" + length +
                ", nullable=" + nullable +
                ", key=" + key +
                ", autoIncrement=" + autoIncrement +
                '}';
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Column) {
            Column that = (Column) obj;
            return this.length.equals(that.getLength())
                    && this.name.equals(that.getName())
                    && this.getTable().getName().equals(that.getTable().getName())
                    && this.nullable == that.isNullable()
                    && this.key == that.isKey()
                    && this.autoIncrement == that.isAutoIncrement();
        }
        return false;
    }

}
