package me.legrange.sql;

import java.sql.JDBCType;

/** Implementation of bit column that is populated by reading from SQL
 *
 */
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
