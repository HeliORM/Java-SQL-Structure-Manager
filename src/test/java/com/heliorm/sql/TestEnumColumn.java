package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Set;

public class TestEnumColumn extends TestColumn implements EnumColumn {

    private final Set<String> enumValues;

    public TestEnumColumn(Table table, String name, boolean nullable, Set<String> enumValues) {
        super(table, name, JDBCType.OTHER, nullable, false, false);
        this.enumValues = enumValues;
    }


    @Override
    public Set<String> getEnumValues() {
        return enumValues;
    }
}
