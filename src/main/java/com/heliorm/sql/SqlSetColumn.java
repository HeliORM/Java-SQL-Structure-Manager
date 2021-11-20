package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Set;


/** Implementation of a set that is populated by reading from SQL */
class SqlSetColumn extends SqlColumn implements SetColumn {

    private final Set<String> setValues;

    SqlSetColumn(Table table, String name, boolean nullable, Set<String> setValues) {
        super(table, name, JDBCType.OTHER, nullable, false);
        this.setValues = setValues;
    }

    @Override
    public Set<String> getSetValues() {
        return setValues;
    }
}
