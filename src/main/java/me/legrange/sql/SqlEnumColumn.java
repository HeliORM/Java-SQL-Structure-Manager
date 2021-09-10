package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;
import java.util.Set;

public class SqlEnumColumn extends SqlColumn implements EnumColumn{

    private final Set<String> enumValues;

    SqlEnumColumn(Table table, String name, boolean nullable, Set<String> enumValues) {
        super(table, name, JDBCType.OTHER, Optional.empty(), nullable, false);
        this.enumValues = enumValues;
    }

    @Override
    public Set<String> getEnumValues() {
        return enumValues;
    }
}
