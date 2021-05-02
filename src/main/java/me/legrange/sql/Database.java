package me.legrange.sql;

import java.util.Set;

public interface Database {

    String getName();

    Set<Table> getTables();

}
