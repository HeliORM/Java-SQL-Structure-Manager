package me.legrange.sql;

import java.util.HashSet;
import java.util.Set;

class TestTable implements Table {

   private final Database database;
   private final String name;
   private final Set<Column> columns;
   private final Set<Index> indexes;

    public TestTable(Database database, String name) {
        this.database = database;
        this.name = name;
        this.columns = new HashSet<>();
        this.indexes = new HashSet<>();
    }

    void addColumn(Column column) {
        columns.add(column);
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
        return columns;
    }

    @Override
    public Set<Index> getIndexes() {
        return indexes;
    }
}
