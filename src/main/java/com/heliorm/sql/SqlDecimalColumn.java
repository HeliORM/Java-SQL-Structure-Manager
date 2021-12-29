package com.heliorm.sql;

import java.sql.JDBCType;

/** Implementation of decimal column that is populated by reading from SQL
 *
 */
public final class SqlDecimalColumn extends SqlColumn implements DecimalColumn {

    private int precision;
    private int scale;

    public SqlDecimalColumn(Table table, String name, JDBCType jdbcType, boolean nullable, int precision, int scale) {
        super(table, name, jdbcType, nullable, false);
        this.precision = precision;
        this.scale = scale;
    }

    public SqlDecimalColumn(Table table, String name, JDBCType jdbcType, boolean nullable) {
        super(table, name, jdbcType, nullable, false);
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    public int getScale() {
        return scale;
    }
}
