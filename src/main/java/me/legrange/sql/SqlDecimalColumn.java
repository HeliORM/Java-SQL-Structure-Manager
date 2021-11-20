package me.legrange.sql;

import java.sql.JDBCType;

/** Implementation of decimal column that is populated by reading from SQL
 *
 */
public final class SqlDecimalColumn extends SqlColumn implements DecimalColumn {

    private int precision;
    private int scale;

    public SqlDecimalColumn(Table table, String name, JDBCType jdbcType, boolean nullable, boolean autoIncrement, int precision, int scale) {
        super(table, name, jdbcType, nullable, autoIncrement);
        this.precision = precision;
        this.scale = scale;
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
