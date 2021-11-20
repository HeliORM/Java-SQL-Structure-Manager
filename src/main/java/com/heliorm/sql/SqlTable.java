package com.heliorm.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/** Implementation of Table  that is populated by reading from SQL */
final class SqlTable implements Table {

    private final Database database;
    private final String name;
    private final Map<String, Column> columns = new HashMap<>();
    private final Map<String, Index> indexes = new HashMap<>();

    SqlTable(Database database, String name) {
        this.database = database;
        this.name = name;
    }

    void addColumn(Column column) {
        columns.put(column.getName(), column);
    }

    void addIndex(Index index) {
        indexes.put(index.getName(), index);
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
        return new HashSet(columns.values());
    }

    @Override
    public Set<Index> getIndexes() {
        return new HashSet(indexes.values());
    }

    @Override
    public Column getColumn(String name) {
        return columns.get(name);
    }

    @Override
    public Index getIndex(String name) {
        return indexes.get(name);
    }
}
