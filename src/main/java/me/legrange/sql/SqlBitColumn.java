package me.legrange.sql;

import java.sql.JDBCType;

final class SqlBitColumn extends SqlColumn implements BitColumn {

    private int bits;

    public SqlBitColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement, int bits) {
        super(table, name, jdbcType, nullable, autoIncrement);
        this.bits = bits;
    }

    @Override
    public int getBits() {
        return bits;
    }
}
