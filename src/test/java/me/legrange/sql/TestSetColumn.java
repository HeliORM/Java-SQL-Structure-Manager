package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;
import java.util.Set;

public class TestSetColumn extends TestColumn implements SetColumn {

    private final Set<String> setValues;

    public TestSetColumn(Table table, String name, boolean nullable, Set<String> setValues) {
        super(table, name, JDBCType.OTHER, nullable, false, false);
        this.setValues = setValues;
    }


    @Override
    public Set<String> getSetValues() {
        return setValues;
    }
}
