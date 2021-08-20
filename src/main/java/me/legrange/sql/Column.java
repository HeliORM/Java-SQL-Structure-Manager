package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;
import java.util.Set;

public interface Column {

    String getName();

    JDBCType getJdbcType();

    Table getTable();

    Optional<Integer> getLength();

    boolean isNullable();

    boolean isKey();

    boolean isAutoIncrement();

    Optional<Set<String>> getEnumValues();

}
