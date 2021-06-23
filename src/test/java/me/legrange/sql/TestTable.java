package me.legrange.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class TestTable implements Table {

   private final Database database;
   private final String name;
   private final Map<String, Column> columns;
   private final Set<Index> indexes;

    public TestTable(Database database, String name) {
        this.database = database;
        this.name = name;
        this.columns = new HashMap<>();
        this.indexes = new HashSet<>();
    }

    void addColumn(Column column) {
        columns.put(column.getName(), column);
    }

    void deleteColumn(Column column) {
        columns.remove(column.getName());
    }
    void addIndex(Index index) {
        indexes.add(index);
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
    public Set<Index> getIndexes() {
        return indexes;
    }
}
