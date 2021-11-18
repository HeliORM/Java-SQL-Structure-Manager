package me.legrange.sql.postgres;

import me.legrange.sql.BitColumn;
import me.legrange.sql.BooleanColumn;
import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.DecimalColumn;
import me.legrange.sql.EnumColumn;
import me.legrange.sql.Index;
import me.legrange.sql.SetColumn;
import me.legrange.sql.SqlModeller;
import me.legrange.sql.StringColumn;
import me.legrange.sql.Table;

import java.sql.Connection;
import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;

public final class PostgresModeller extends SqlModeller {
    /**
     * Create a new modeller with the given connection supplier and driver.
     *
     * @param supplier The connection supplier
     */
    public PostgresModeller(Supplier<Connection> supplier) {
        super(supplier);
    }

    @Override
    protected boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName) {
        return jdbcType == JDBCType.VARCHAR && typeName.endsWith("_" + columnName);
    }

    @Override
    protected boolean typesAreCompatible(Column one, Column other) {
        if (one instanceof BooleanColumn) {
            if (other instanceof BitColumn) {
                return ((BitColumn) other).getBits() == 1;
            }
            return other instanceof BooleanColumn;
        }
        if (one instanceof BitColumn) {
            if (other instanceof BitColumn) {
                return ((BitColumn) one).getBits() == ((BitColumn) other).getBits();
            }
            return other instanceof BooleanColumn && ((BitColumn) one).getBits() == 1;
        }
        if (one instanceof StringColumn) {
            if (other instanceof StringColumn) {
                return actualTextLength((StringColumn) one) == actualTextLength((StringColumn) other);
            }
            return false;
        }
        if (one instanceof DecimalColumn) {
            if (other instanceof DecimalColumn) {
                return ((DecimalColumn) one).getPrecision() == ((DecimalColumn) other).getPrecision()
                        && ((DecimalColumn) one).getScale() == ((DecimalColumn) other).getScale();
            }
            return other.getJdbcType() == JDBCType.NUMERIC;
        }
        if (one.getJdbcType() == JDBCType.NUMERIC) {
            switch (other.getJdbcType()) {
                case NUMERIC:
                case DECIMAL:
                    return true;
            }
        }
        return one.getJdbcType() == other.getJdbcType();
    }

    @Override
    protected boolean isSetColumn(String columnName, JDBCType jdbcType, String typeName) {
        return false;
    }

    @Override
    protected String makeReadSetQuery(SetColumn sqlSetColumn) {
        return null;
    }

    @Override
    protected Set<String> extractSetValues(String string) {
        return null;
    }


    @Override
    public String makeModifyColumnQuery(Column column) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("ALTER TABLE %s", getTableName(column.getTable())));
        sql.append(format("ALTER %s DROP DEFAULT", getColumnName(column)));
        sql.append(format(",ALTER %s TYPE %s USING(%s::text::%s)",
                getColumnName(column), createBasicType(column),
                getColumnName(column),
                typeName(column)));
        if (!column.isNullable()) {
            sql.append(format(",ALTER %s SET NOT NULL", getColumnName(column)));
        } else {
            sql.append(format(",ALTER %s DROP NOT NULL", getColumnName(column)));
        }
        return sql.toString();
    }

    @Override
    public String getDatabaseName(Database database) {
        return format("\"%s\"", database.getName());
    }

    @Override
    public String getTableName(Table table) {
        return format("\"%s\"", table.getName());
    }

    @Override
    public String getCreateType(Column column) {
        StringBuilder type = new StringBuilder();
        type.append(createBasicType(column));
        if (column.isKey()) {
            type.append(" PRIMARY KEY");
        }
        if (!column.isNullable()) {
            type.append(" NOT NULL");
        }
        return type.toString();
    }

    @Override
    public String makeRemoveIndexQuery(Index index) {
        return format("DROP INDEX IF EXISTS %s", getIndexName(index));
    }

    @Override
    public String makeModifyIndexQuery(Index index) {
       return  makeRemoveIndexQuery(index) + ";" + makeAddIndexQuery(index);
    }

    @Override
    public String getColumnName(Column column) {
        return format("\"%s\"", column.getName());
    }

    @Override
    public String getIndexName(Index index) {
        return format("\"%s\"", index.getName());
    }

    @Override
    public boolean supportsAlterIndex() {
        return true;
    }

    @Override
    public String makeReadEnumQuery(EnumColumn column) {
        return format("SELECT ENUM_RANGE(NULL::\"%s\")", typeName(column));
    }

    @Override
    public Set<String> extractEnumValues(String text) {
        return Arrays.stream(text.replace("{", "").replace("}", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    @Override
    public String makeAddColumnQuery(Column column) {
        StringBuilder buf = new StringBuilder();
        if (column instanceof EnumColumn) {
            buf.append(makeAddEnumTypeQuery((EnumColumn) column));
        }
        buf.append(format("ALTER TABLE %s ADD COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column)));
        return buf.toString();
    }

    private String makeAddEnumTypeQuery(EnumColumn column) {
        String typeName = typeName(column);
        StringJoiner buf = new StringJoiner("\n");
        buf.add("DO $$");
        buf.add("BEGIN");
        buf.add(format("    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '%s') THEN", typeName));
        buf.add(format("        CREATE TYPE \"%s\" AS ENUM(", typeName));
        buf.add(column.getEnumValues().stream()
                .map(v -> "'" + v + "'")
                .reduce((a, b) -> a + "," + b).get());
        buf.add(");");
        buf.add("    END IF;");
        buf.add("END$$;");
        return buf.toString();
    }

    /**
     * Create the basic type declaration for a column excluding annotations like keys and nullability
     *
     * @param column The column
     * @return The type declaration
     */
    private String createBasicType(Column column) {
        StringBuilder type = new StringBuilder();
        String typeName;
        if (column instanceof EnumColumn) {
            typeName = "\"" + typeName(column) + "\"";
        } else if (column instanceof StringColumn) {
            int length = ((StringColumn) column).getLength();
            if (length > 65535) {
                typeName = "TEXT";
            } else {
                typeName = format("VARCHAR(%d)", length);
            }
        } else {
            switch (column.getJdbcType()) {
                case TINYINT:
                    if (column.isKey() && column.isAutoIncrement()) {
                        typeName = "SERIAL";
                    } else {
                        typeName = "TINYINT";
                    }
                    break;
                case SMALLINT:
                    if (column.isKey() && column.isAutoIncrement()) {
                        typeName = "SERIAL";
                    } else {
                        typeName = "SMALLINT";
                    }
                    break;
                case INTEGER:
                    if (column.isKey() && column.isAutoIncrement()) {
                        typeName = "SERIAL";
                    } else {
                        typeName = "INTEGER";
                    }
                    break;
                case BIGINT:
                    if (column.isKey() && column.isAutoIncrement()) {
                        typeName = "BIGSERIAL";
                    } else {
                        typeName = "BIGINT";
                    }
                    break;
                default:
                    typeName = column.getJdbcType().getName();
            }
        }
        type.append(typeName);
        return type.toString();
    }

    private String typeName(Column column) {
        if (column instanceof EnumColumn) {
            return format("%s_%s", column.getTable().getName(), column.getName());
        }
        switch (column.getJdbcType()) {
            case TINYINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    return "SERIAL";
                } else {
                    return "TINYINT";
                }
            case SMALLINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    return "SERIAL";
                } else {
                    return "SMALLINT";
                }
            case INTEGER:
                if (column.isKey() && column.isAutoIncrement()) {
                    return "SERIAL";
                } else {
                    return "INTEGER";
                }
            case BIGINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    return "BIGSERIAL";
                } else {
                    return "BIGINT";
                }
            default:
                return column.getJdbcType().getName();
        }

    }

}
