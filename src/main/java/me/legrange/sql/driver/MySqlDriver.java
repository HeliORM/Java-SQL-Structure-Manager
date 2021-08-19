package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

import java.sql.JDBCType;

import static java.lang.String.format;

public class MySqlDriver extends GenericSqlDriver {

    public MySqlDriver() {
    }

    @Override
    public String getDatabaseName(Database database) {
        return format("`%s`", database.getName());
    }

    @Override
    public String getTableName(Table table) {
        return format("`%s`", table.getName());
    }

    @Override
    public String getCreateType(Column column) {
        StringBuilder type = new StringBuilder();
        String typeName = column.getJdbcType().getName();
        boolean useLength  = false;
        switch (column.getJdbcType()) {
            case LONGVARCHAR:
            case VARCHAR:
                if (column.getLength().isPresent()) {
                   int  length = column.getLength().get();
                    if (length >= 16777215) {
                        typeName = "LONGTEXT";
                    } else if (length > 65535) {
                        typeName = "MEDIUMTEXT";
                    } else if (length > 255) {
                        typeName = "TEXT";
                    } else {
                        typeName = "VARCHAR";
                        useLength = true;
                    }
                }
                break;
        }
        type.append(typeName);
        if (useLength) {
            type.append(format("(%d)", column.getLength().get()));
        }
        if (!column.isNullable()) {
            type.append(" NOT NULL");
        }
        if (column.isAutoIncrement()) {
            type.append(" AUTO_INCREMENT");
        }
        if (column.isKey()) {
            type.append(" PRIMARY KEY");
        }
        return type.toString();
    }

    @Override
    public String getColumnName(Column column) {
        return format("`%s`", column.getName());
    }

    @Override
    public String getIndexName(Index index) {
        return format("`%s`", index.getName());
    }

    @Override
    public boolean supportsAlterIndex() {
        return false;
    }

}
