package com.heliorm.sql;

import java.sql.JDBCType;
import java.util.Set;


/** Implementation of a set that is populated by reading from SQL */
class SqlSetColumn extends SqlColumn implements SetColumn {

    private final Set<String> setValues;

    SqlSetColumn(Table table, String name, boolean nullable, String defVal, Set<String> setValues) {
        super(table, name, JDBCType.OTHER, nullable,  defVal,false);
        this.setValues = setValues;
    }

    @Override
    public Set<String> getSetValues() {
        return setValues;
    }
}
