package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;

public class TestColumn implements Column {

    private final Table table;
    private final String name;
    private final JDBCType jdbcType;
    private final Optional<Integer> length;
    private final boolean nullable;
    private final boolean key;
    private final boolean autoIncrement;

    public TestColumn(Table table, String name, JDBCType jdbcType) {
        this(table, name, jdbcType, Optional.empty(), false, false, false);
    }

    public TestColumn(Table table, String name, JDBCType jdbcType, Optional<Integer> length, boolean nullable, boolean key, boolean autoIncrement) {
        this.table = table;
        this.name = name;
        this.jdbcType = jdbcType;
        this.length = length;
        this.nullable  = nullable;
        this.key = key;
        this.autoIncrement = autoIncrement;
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
    public Table getTable() {
        return table;
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

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (jdbcType != null ? jdbcType.hashCode() : 0);
        result = 31 * result + (length != null ? length.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "TestColumn{" +
                "table=" + table +
                ", name='" + name + '\'' +
                ", jdbcType=" + jdbcType +
                ", length=" + length +
                ", nullable=" + nullable +
                ", key=" + key +
                ", autoIncrement=" + autoIncrement +
                '}';
    }
}
