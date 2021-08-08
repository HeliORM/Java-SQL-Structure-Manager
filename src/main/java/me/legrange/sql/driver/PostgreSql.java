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
        sql.append(format("ALTER TABLE %s ALTER COLUMN %s TYPE %s;",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column)));
        if (column.isNullable()) {
            sql.append(format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
                    getTableName(column.getTable()),
                    getColumnName(column)));
        }
        else {
            sql.append(format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL",
                    getTableName(column.getTable()),
                    getColumnName(column)));

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
        String typeName;
        switch (column.getJdbcType()) {
            case TINYINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    typeName =  "SERIAL";
                }
                else {
                    typeName = "TINYINT";
                }
                break;
            case SMALLINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    typeName =  "SERIAL";
                }
                else {
                    typeName =  "SMALLINT";
                }
                break;
            case INTEGER:
                if (column.isKey() && column.isAutoIncrement()) {
                    typeName =  "SERIAL";
                }
                else {
                    typeName =  "INTEGER";
                }
                break;
            case BIGINT:
                if (column.isKey() && column.isAutoIncrement()) {
                    typeName =  "BIGSERIAL";
                }
                else {
                    typeName = "BIGINT";
                }
                break;
            default:
                typeName = column.getJdbcType().getName();
        }
        type.append(typeName);
        if (column.getLength().isPresent()) {
            type.append(format("(%d)", column.getLength().get()));
        }
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
}
