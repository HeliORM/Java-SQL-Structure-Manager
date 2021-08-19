package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

import static java.lang.String.format;

public final class PostgreSql extends GenericSqlDriver {

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
        StringBuilder sql = new StringBuilder();
        sql.append(makeRemoveIndexQuery(index));
        sql.append(";");
        sql.append(makeAddIndexQuery(index));
        return sql.toString();
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

    /**
     * Create the basic type declaration for a coloumn excluding annotations like keys and nullability
     *
     * @param column The column
     * @return The type declaration
     */
    private String createBasicType(Column column) {
        StringBuilder type = new StringBuilder();
        String typeName = null;
        boolean useLength = false;
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
            case LONGVARCHAR:
            case VARCHAR:
                if (column.getLength().isPresent()) {
                    int length = column.getLength().get();
                    if (length > 65535) {
                        typeName = "TEXT";
                        useLength = false;
                    } else {
                        typeName = "VARCHAR";
                        useLength = true;
                    }
                }
                break;
            default:
                typeName = column.getJdbcType().getName();
        }
        type.append(typeName);
        if (useLength) {
            type.append(format("(%d)", column.getLength().get()));
        }
        return type.toString();
    }

    private String typeName(Column column) {
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
