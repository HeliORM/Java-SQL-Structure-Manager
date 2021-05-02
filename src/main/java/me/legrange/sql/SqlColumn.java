package me.legrange.sql;

import java.sql.JDBCType;

class SqlColumn implements Column {

    private final String name;
    private final JDBCType jdbcType;
   private final Class<?> javaType;

     SqlColumn(String name, JDBCType jdbcType, Class<?> javaType) {
        this.name = name;
        this.jdbcType = jdbcType;
        this.javaType = javaType;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public JDBCType getJdbcType() {
        return jdbcType;
    }

    @Override
    public Class<?> getJavaType() {
        return javaType;
    }
}
