package me.legrange.sql;

import java.util.Set;

final class TestIndex implements Index {

    private final Table table;
    private final String name;
    private final boolean unique;
    private final Set<Column> columns;

    public TestIndex(Table table, String name, boolean unique, Set<Column> columns) {
        this.table = table;
        this.name = name;
        this.unique = unique;
        this.columns = columns;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Table getTable() {
        return table;
    }

    @Override
    public Set<Column> getColumns() {
        return columns;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }
}