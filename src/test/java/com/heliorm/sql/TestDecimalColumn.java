package com.heliorm.sql;

import java.sql.JDBCType;

public class TestDecimalColumn extends TestColumn implements DecimalColumn{
    private final int precision;
    private final  int scale;

    public TestDecimalColumn(Table table, String name, JDBCType jdbcType) {
        super(table, name, jdbcType);
        this.precision = 18;
        this.scale = 2;
    }

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
