package me.legrange.sql;

import java.sql.JDBCType;

public interface Column {

    String getName();

    JDBCType getJdbcType();

    Class<?> getJavaType();

    Table getTable();


}
