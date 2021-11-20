package me.legrange.sql;

import java.util.HashSet;
import java.util.Set;

/** Implementation of Database that is populated by reading from SQL
 *
 */
final class SqlDatabase implements Database {

    private final String name;
    private final Set<Table> tables = new HashSet<>();

    public SqlDatabase(String name) {
        this.name = name;
    }

    void addTable(Table table) {
        tables.add(table);
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
