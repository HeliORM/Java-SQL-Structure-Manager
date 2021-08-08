package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

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
        type.append(typeName);
        if (column.getLength().isPresent()) {
            type.append(format("(%d)", column.getLength().get()));
        }
        if (!column.isNullable()) {
            type.append(" NOT NULL");
        }
        if (column.isAutoIncrement()) {
            type.append( " AUTO_INCREMENT");
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
