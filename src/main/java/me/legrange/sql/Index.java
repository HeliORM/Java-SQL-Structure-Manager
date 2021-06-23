package me.legrange.sql;

import java.util.Set;

public interface Index {

    String getName();

    Table getTable();

    Set<Column> getColumns();

    boolean isUnique();

}
