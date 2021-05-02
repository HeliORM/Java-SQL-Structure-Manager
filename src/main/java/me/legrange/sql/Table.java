package me.legrange.sql;

import java.util.Set;

public interface Table {

    Database getDatabase();

    String getName();

    Set<Column> getColumns();

    Set<Index> getIndexes();
}
