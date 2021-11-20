package com.heliorm.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TestTable implements Table {

   private final Database database;
   private final String name;
   private final Map<String, Column> columns;
   private final Map<String, Index> indexes;

    public TestTable(Database database, String name) {
        this.database = database;
        this.name = name;
        this.columns = new HashMap<>();
        this.indexes = new HashMap<>();
    }

    void addColumn(Column column) {
        columns.put(column.getName(), column);
    }

    void deleteColumn(Column column) {
        columns.remove(column.getName());
    }

    void addIndex(Index index) {
        indexes.put(index.getName(), index);
    }

    void removeIndex(Index index) {
        indexes.remove(index.getName());
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
        return new HashSet<>(columns.values());
    }

    @Override
    public Column getColumn(String name) {
        return columns.get(name);
    }

    @Override
    public Set<Index> getIndexes() {
        return new HashSet<>(indexes.values());
    }

    @Override
    public Index getIndex(String name) {
        return indexes.get(name);
    }
}
