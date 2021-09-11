package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;
import java.util.Set;

public class SqlSetColumn extends SqlColumn implements SetColumn{

    private final Set<String> setValues;

    SqlSetColumn(Table table, String name, boolean nullable, Set<String> setValues) {
        super(table, name, JDBCType.OTHER, Optional.empty(), nullable, false);
        this.setValues = setValues;
    }

    @Override
    public Set<String> getSetValues() {
        return setValues;
    }
}
