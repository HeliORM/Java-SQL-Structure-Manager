package me.legrange.sql;

import java.util.HashSet;
import java.util.Set;

public class TestDatabase implements Database {

    private final String name;
    private final Set<Table> tables;

    public TestDatabase(String name) {
        this.name = name;
        this.tables = new HashSet<>();
    }

    void addTable(Table table) {
        this.tables.add(table);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Set<Table> getTables() {
        return tables;
    }
}
