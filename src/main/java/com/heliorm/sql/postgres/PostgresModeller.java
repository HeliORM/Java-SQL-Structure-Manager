package com.heliorm.sql.postgres;

import com.heliorm.sql.BitColumn;
import com.heliorm.sql.BooleanColumn;
import com.heliorm.sql.Database;
import com.heliorm.sql.DecimalColumn;
import com.heliorm.sql.EnumColumn;
import com.heliorm.sql.SqlModeller;
import com.heliorm.sql.SqlModellerException;
import com.heliorm.sql.Column;
import com.heliorm.sql.Index;
import com.heliorm.sql.SetColumn;
import com.heliorm.sql.StringColumn;
import com.heliorm.sql.Table;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
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
    public void modifyColumn(Column current) throws SqlModellerException {
        if (current instanceof EnumColumn) {
            modifyEnumColumn((EnumColumn) current);
        } else {
            super.modifyColumn(current);
        }
    }

    @Override
    public void modifyIndex(Index index) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeModifyIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error modifying index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    @Override
    public boolean supportsSet() {
        return false;
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
    protected String getDatabaseName(Database database) {
        return format("\"%s\"", database.getName());
    }

    @Override
    protected String getTableName(Table table) {
        return format("\"%s\"", table.getName());
    }

    @Override
    protected String getCreateType(Column column) {
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
    protected String makeRemoveIndexQuery(Index index) {
        return format("DROP INDEX IF EXISTS %s", getIndexName(index));
    }

    @Override
    protected String makeModifyIndexQuery(Index index) {
        return makeRemoveIndexQuery(index) + ";" + makeAddIndexQuery(index);
    }

    @Override
    protected String getColumnName(Column column) {
        return format("\"%s\"", column.getName());
    }

    @Override
    protected String getIndexName(Index index) {
        return format("\"%s\"", index.getName());
    }

    @Override
    protected String makeReadEnumQuery(EnumColumn column) {
        return format("SELECT ENUM_RANGE(NULL::\"%s\")", typeName(column));
    }

    @Override
    protected Set<String> extractEnumValues(String text) {
        return Arrays.stream(text.replace("{", "").replace("}", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    @Override
    protected String makeAddColumnQuery(Column column) {
        StringBuilder buf = new StringBuilder();
        if (column instanceof EnumColumn) {
            buf.append(makeAddEnumTypeQuery((EnumColumn) column));
        } else if (column instanceof SetColumn) {
            buf.append(makeAddSetTypeQuery((SetColumn) column));
        }
        buf.append(format("ALTER TABLE %s ADD COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column)));
        return buf.toString();
    }

    @Override
    protected String makeCreateTableQuery(Table table) throws SqlModellerException {
        StringBuilder head = new StringBuilder();
        StringJoiner body = new StringJoiner(",");
        for (Column column : table.getColumns()) {
            if (column instanceof EnumColumn) {
                head.append(makeAddEnumTypeQuery((EnumColumn) column));
            }
            if (column instanceof SetColumn) {
                throw new SqlModellerException(format("SET data types are not supported for Postgres"));
            }
            body.add(format("%s %s", getColumnName(column), getCreateType(column)));
        }
        StringBuilder sql = new StringBuilder(head.toString());
        sql.append(format("CREATE TABLE %s (", getTableName(table)));
        sql.append(body);
        sql.append(")");
        return sql.toString();
    }

    private void modifyEnumColumn(EnumColumn column) throws SqlModellerException {
        Set<String> want = column.getEnumValues();
        Set<String> have = readEnumValues(column);
        if (!want.equals(have)) {
            StringJoiner query = new StringJoiner(";");
            query.add(format("ALTER TYPE %s RENAME TO %s_old", typeName(column), typeName(column)));
            query.add(makeAddEnumTypeQuery(column));
            query.add(format("ALTER TABLE %s COLUMN %s TYPE %s USING %s::text::%s",
                    getTableName(column.getTable()),
                    getColumnName(column),
                    typeName(column),
                    getColumnName(column),
                    typeName(column)));
        }

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

    private String makeAddSetTypeQuery(SetColumn column) {
        String typeName = typeName(column);
        StringJoiner buf = new StringJoiner("\n");
        buf.add("DO $$");
        buf.add("BEGIN");
        buf.add(format("    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '%s') THEN", typeName));
        buf.add(format("        CREATE TYPE \"%s\" AS SET(", typeName));
        buf.add(column.getSetValues().stream()
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
        } else if (column instanceof SetColumn) {
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

    protected String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER INDEX %s RENAME to %s",
                getIndexName(current),
                getIndexName(changed));
    }

}
