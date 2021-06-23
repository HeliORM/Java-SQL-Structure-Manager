package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.Driver;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

import java.sql.JDBCType;

import static java.lang.String.format;

public class PostgreSql implements Driver {
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
            case BIT :
            case BOOLEAN:
                typeName = JDBCType.BIT.getName();
                break;
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
        if (!column.isNullable()) {
            type.append(" NOT NULL");
        }
        return type.toString();
    }

    @Override
    public String getColumnName(Column column) {
        return format("\"%s\"", column.getName());
    }

    @Override
    public String getIndexName(Index index) {
        return format("\"%s\"", index.getName());
    }
}
