package me.legrange.sql;

import java.sql.JDBCType;

final class SqlBitColumn extends SqlColumn implements BitColumn {

    private int bits;

    public SqlBitColumn(Table table, String name, boolean nullable, int bits) {
        super(table, name, JDBCType.BIT, nullable, false);
        this.bits = bits;
    }

    @Override
    public int getBits() {
        return bits;
    }
}
