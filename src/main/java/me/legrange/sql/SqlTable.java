package me.legrange.sql;

import java.util.HashSet;
import java.util.Set;

final class SqlTable implements Table {

    private final Database database;
    private final String name;
    private final Set<Column> columns = new HashSet<>();
    private final Set<Index> indexes = new HashSet<>();

    SqlTable(Database database, String name) {
        this.database = database;
        this.name = name;
    }

    void addColumn(Column column) {
        this.columns.add(column);
    }

    void addIndex(Index index) {
        this.indexes.add(index);
    }

    @Override
    public Database getDatabase() {
        return database;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<Column> getColumns() {
        return columns;
    }

    @Override
    public Set<Index> getIndexes() {
        return indexes;
    }
}
