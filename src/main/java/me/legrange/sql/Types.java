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


    static {
        addType(JDBCType.ARRAY, byte[].class);

        addType(JDBCType.BIT, Boolean.class);
        addType(JDBCType.BIGINT, Long.class);
        addType(JDBCType.BINARY, byte[].class);
        addType(JDBCType.BOOLEAN, Boolean.class);
        addType(JDBCType.BLOB, byte[].class);

        addType(JDBCType.CHAR, String.class);
        addType(JDBCType.CLOB, byte[].class);

        addType(JDBCType.DATE, Date.class);
        addType(JDBCType.DECIMAL, Double.class);
        addType(JDBCType.DOUBLE, Double.class);

        addType(JDBCType.FLOAT, Double.class);

        addType(JDBCType.INTEGER, Integer.class);

        addType(JDBCType.LONGVARBINARY, byte[].class);
        addType(JDBCType.LONGNVARCHAR, String.class);
        addType(JDBCType.LONGVARCHAR, String.class);

        addType(JDBCType.NCLOB, byte[].class);
        addType(JDBCType.NCHAR, Character.class);
        addType(JDBCType.NUMERIC, Double.class);

        addType(JDBCType.REAL, Float.class);

        addType(JDBCType.SMALLINT, Integer.class);

        addType(JDBCType.TIME, Time.class);
        addType(JDBCType.TIMESTAMP, Timestamp.class);
        addType(JDBCType.TINYINT, Integer.class);

        addType(JDBCType.VARBINARY, byte[].class);
        addType(JDBCType.VARCHAR, String.class);

    }
}
