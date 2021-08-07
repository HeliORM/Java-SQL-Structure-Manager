package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Driver;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

import java.util.StringJoiner;

import static java.lang.String.format;

abstract class GenericSqlDriver implements Driver {

    @Override
    public String makeCreateTableQuery(Table table) {
        StringJoiner body = new StringJoiner(",");
        for (Column column : table.getColumns()) {
            body.add(format("%s %s", getColumnName(column), getCreateType(column)));
        }
        StringBuilder sql = new StringBuilder();
        sql.append(format("CREATE TABLE %s (", getTableName(table)));
        sql.append(body.toString());
        sql.append(")");
        return sql.toString();
    }

    @Override
    public String makeRemoveTableQuery(Table table) {
        return format("DROP TABLE %s", getTableName(table));
    }

    @Override
    public String makeDeleteTableQuery(Table table) {
        return format("DROP TABLE %s", getTableName(table));
    }

    @Override
    public String makeRemoveIndexQuery(Index index) {
        return format("DROP INDEX %s on %s",
                getIndexName(index),
                getTableName(index.getTable()));
    }

    @Override
    public String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER TABLE INDEX %s RENAME INDEX %s to %s",
                getTableName(current.getTable()),
                getIndexName(current),
                getIndexName(changed));
    }

    @Override
    public String makeModifyIndexQuery(Index index) {
        return format("ALTER %sINDEX %s ON %s %",
                index.isUnique() ? "UNIQUE " : "",
                getIndexName(index),
                getTableName(index.getTable()),
                index.getColumns().stream()
                        .map(column -> getColumnName(column))
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }


    @Override
    public String makeRenameColumnQuery(Column column, Column changed) {
        return format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getColumnName(changed));
    }

    @Override
    public String makeDeleteColumnQuery(Column column) {
        return format("ALTER TABLE %s DROP COLUMN %s",
                getTableName(column.getTable()),
                getColumnName(column));
    }

    @Override
    public String makeModifyColumnQuery(Column column) {
        return format("ALTER TABLE %s MODIFY COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column));
    }

    @Override
    public String makeAddColumnQuery(Column column) {
        return format("ALTER TABLE %s ADD COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column));
    }

    @Override
    public String makeAddIndexQuery(Index index) {
        return format("CREATE %sINDEX %s on %s (%s)",
                index.isUnique() ? "UNIQUE " : "",
                getIndexName(index),
                getTableName(index.getTable()),
                index.getColumns().stream()
                        .map(column -> getColumnName(column))
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }


}
