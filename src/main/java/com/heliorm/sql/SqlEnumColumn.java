package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Set;

/** Implementation of enum column that is populated by reading from SQL
 *
 */
class SqlEnumColumn extends SqlColumn implements EnumColumn{

    private final Set<String> enumValues;

    SqlEnumColumn(Table table, String name, boolean nullable, String defaultValue, Set<String> enumValues) {
        super(table, name, JDBCType.OTHER,  nullable, defaultValue, false);
        this.enumValues = enumValues;
    }

    @Override
    public Set<String> getEnumValues() {
        return enumValues;
    }
}
