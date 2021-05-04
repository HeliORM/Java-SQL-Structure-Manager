package me.legrange.sql;


import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import static java.lang.String.format;
class Types {

    private static final Map<JDBCType, Class<?>> jdbcTypeToClass = new HashMap<>();

    private static void addType( JDBCType jdbcType, Class<?> javaType) {
        jdbcTypeToClass.put(jdbcType, javaType);
    }

    static Class<?> findJavaType(JDBCType jdbcType) throws SqlManagerException {
        if (!jdbcTypeToClass.containsKey(jdbcType)) {
            throw new SqlManagerException(format("Cannot determine Java type for JDBC type '%s'", jdbcType));
        }
        return jdbcTypeToClass.get(jdbcType);
    }

    static {
        addType(JDBCType.CHAR, String.class);
        addType(JDBCType.VARCHAR, String.class);
        addType(JDBCType.LONGNVARCHAR, String.class);
        addType(JDBCType.NUMERIC, BigDecimal.class);
        addType(JDBCType.DECIMAL, BigDecimal.class);
        addType(JDBCType.BIT, Boolean.class);
        addType(JDBCType.TINYINT, Integer.class);
        addType(JDBCType.SMALLINT, Integer.class);
        addType(JDBCType.INTEGER, Integer.class);
        addType(JDBCType.BIGINT, Long.class);
        addType(JDBCType.REAL, Float.class);
        addType(JDBCType.FLOAT, Double.class);
        addType(JDBCType.DOUBLE, Double.class);
        addType(JDBCType.BINARY, byte[].class);
        addType(JDBCType.VARBINARY, byte[].class);
        addType(JDBCType.LONGVARBINARY, byte[].class);
        addType(JDBCType.DATE, Date.class);
        addType(JDBCType.TIME, Time.class);
        addType(JDBCType.TIMESTAMP, Timestamp.class);
    }
}
