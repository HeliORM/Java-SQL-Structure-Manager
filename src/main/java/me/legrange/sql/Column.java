package me.legrange.sql;

import java.sql.JDBCType;
import java.util.Optional;

public interface Column {

    String getName();

    JDBCType getJdbcType();

    Class<?> getJavaType();

    Table getTable();

    Optional<Integer> getLength();

    boolean isNullable();

    boolean isKey();

    boolean isAutoIncrement();


}
