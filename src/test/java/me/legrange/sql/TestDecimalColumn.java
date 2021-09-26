package me.legrange.sql;

import java.sql.JDBCType;

public class TestDecimalColumn extends TestColumn implements DecimalColumn{
    private int precision;
    private int scale;

    public TestDecimalColumn(Table table, String name, int precision, int scale) {
        super(table, name, JDBCType.DECIMAL);
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
