package com.heliorm.sql.postgres;

import com.heliorm.sql.BitColumn;
import com.heliorm.sql.BooleanColumn;
import com.heliorm.sql.Column;
import com.heliorm.sql.Database;
import com.heliorm.sql.DecimalColumn;
import com.heliorm.sql.EnumColumn;
import com.heliorm.sql.Index;
import com.heliorm.sql.SetColumn;
import com.heliorm.sql.SqlModeller;
import com.heliorm.sql.SqlModellerException;
import com.heliorm.sql.StringColumn;
import com.heliorm.sql.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

/**
 * An implementation of the SQL modeller that deals with PostgreSQL syntax.
 */
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
    public void modifyColumn(Column column) throws SqlModellerException {
        if (column instanceof EnumColumn) {
            modifyEnumColumn((EnumColumn) column);
        } else {
            super.modifyColumn(column);
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
    protected boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName) throws SqlModellerException {
        if (jdbcType == JDBCType.VARCHAR) {
            try (Connection con = con(); Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(makeReadEnumQuery(typeName))) {
                return rs.next();
            } catch (SQLException ex) {
                throw new SqlModellerException(format("Error reading enum values from databases (%s)", ex.getMessage()), ex);
            }
        }
        return false;
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
                if (one.getJdbcType() == JDBCType.DOUBLE) {
                    if (other.getJdbcType() == JDBCType.DOUBLE) {
                        return true;
                    }
                }
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
    protected String makeReadSetQuery(SetColumn sqlSetColumn) throws SqlModellerException {
            throw new SqlModellerException(format("SET data types are not supported for PostgreSQL"));
    }

    @Override
    protected Set<String> extractSetValues(String string) {
        return null;
    }

    @Override
    public String makeModifyColumnQuery(Column column) throws SqlModellerException {
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
    protected String getCreateType(Column column) throws SqlModellerException {
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
    protected String makeAddColumnQuery(Column column) throws SqlModellerException {
        StringBuilder buf = new StringBuilder();
        if (column instanceof EnumColumn) {
            buf.append(makeAddEnumTypeQuery((EnumColumn) column));
        } else if (column instanceof SetColumn) {
            throw new SqlModellerException(format("SET data types are not supported for PostgreSQL"));
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
                throw new SqlModellerException(format("SET data types are not supported for PostgreSQL"));
            }
            body.add(format("%s %s", getColumnName(column), getCreateType(column)));
        }
        StringBuilder sql = new StringBuilder(head.toString());
        sql.append(format("CREATE TABLE %s (", getTableName(table)));
        sql.append(body);
        sql.append(")");
        for (Index index : table.getIndexes()) {
            sql.append(";\n");
            sql.append(makeAddIndexQuery(index));
        }
        return sql.toString();
    }

    @Override
    protected String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER INDEX %s RENAME to %s",
                getIndexName(current),
                getIndexName(changed));
    }

    @Override
    protected Set<String> readEnumValues(EnumColumn column) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(makeReadEnumQuery(getSqlTypeName(column)))) {
            if (rs.next()) {
                return Stream.of(rs.getString("enum_value").split(","))
                        .map(text -> text.trim())
                        .collect(Collectors.toSet());
            }
            throw new SqlModellerException(format("No enum values found for column %s in table %s ", column.getName(), column.getTable().getName()));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error reading enum values (%s)", ex.getMessage()), ex);
        }

    }

    /** Read the SQL type name for the give column from the database meta data.
     *
     * @param column The column
     * @return The type name
     * @throws SqlModellerException
     */
    private String getSqlTypeName(Column column) throws SqlModellerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet rs = dbm.getColumns(column.getTable().getDatabase().getName(), null, column.getTable().getName(), column.getName())) {
                if (rs.next()) {
                   return rs.getString("TYPE_NAME");
                }
            }
            throw new SqlModellerException(format("Column %s found in table %s ", column.getName(), column.getTable().getName()));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error determining SQL type name for column %s in table %s ", column.getName(), column.getTable().getName()));
        }
    }

    /**
     * Modify an enum colum in a PostgreSQL specific way.
     *
     * @param column The column to modify
     * @throws SqlModellerException Thrown if it goes worng
     */
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

    /**
     * Generate an SQL statement to modify a PostgreSQL enum type.
     *
     * @param column The column
     * @return The SQL
     */
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
    private String createBasicType(Column column) throws SqlModellerException {
        StringBuilder type = new StringBuilder();
        String typeName;
        if (column instanceof EnumColumn) {
            typeName = "\"" + typeName(column) + "\"";
        } else if (column instanceof SetColumn) {
            throw new SqlModellerException(format("SET data types are not supported for Postgres"));
        } else if (column instanceof StringColumn) {
            int length = ((StringColumn) column).getLength();
            if (length > 65535) {
                typeName = "TEXT";
            } else {
                typeName = format("VARCHAR(%d)", length);
            }
        } else if (column instanceof DecimalColumn) {
            switch (column.getJdbcType()) {
                case DOUBLE:
                    typeName = format("DOUBLE PRECISION");
                    break;
                case FLOAT:
                    typeName = format("FLOAT(%d)", ((DecimalColumn) column).getPrecision());
                    break;
                case NUMERIC:
                case DECIMAL:
                    typeName = format("DECIMAL(%d,%d)", ((DecimalColumn) column).getPrecision(), ((DecimalColumn) column).getScale());
                    break;
                default:
                    throw new SqlModellerException(format("Unexpected JDBC type %s in decimal column", column.getJdbcType()));
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
                case DOUBLE:
                    typeName = "DOUBLE PRECISION";
                    break;
                default:
                    typeName = column.getJdbcType().getName();
            }
        }
        type.append(typeName);
        return type.toString();
    }

    /**
     * Determine the PostgreSQL type name for a column.
     *
     * @param column The column
     * @return The type name
     */
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

    /**
     * Generate an SQL query that reads the enum data for the given type name.
     *
     * @param typeName The type name
     * @return The SQL query
     */
    private String makeReadEnumQuery(String typeName) {
        return "select n.nspname as enum_schema,  \n" +
                "    t.typname as enum_name,\n" +
                "    string_agg(e.enumlabel, ', ') as enum_value\n" +
                "from pg_type t \n" +
                "    join pg_enum e on t.oid = e.enumtypid  \n" +
                "    join pg_catalog.pg_namespace n ON n.oid = t.typnamespace\n" +
                "    where t.typname = '" + typeName + "' " +
                "group by enum_schema, enum_name;";
    }

}
