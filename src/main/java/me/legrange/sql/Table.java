package me.legrange.sql;

import java.util.Set;

public interface Table {

    Database getDatabase();

    String getName();

    Set<Column> getColumns();

    Column getColumn(String name);

    Set<Index> getIndexes();

    Index getIndex(String name);
}
