package com.heliorm.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class TestIndex implements Index {

    private final Table table;
    private final String name;
    private final boolean unique;
    private final Map<String, Column> columns;

    public TestIndex(Table table, String name, boolean unique) {
        this.table = table;
        this.name = name;
        this.unique = unique;
        this.columns = new HashMap();
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
        return new HashSet<>(columns.values());
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    void addColumn(Column column) {
        columns.put(column.getName(), column);
    }
}