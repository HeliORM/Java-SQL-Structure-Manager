package me.legrange.sql;

public interface Driver {

     String getDatabaseName(Database database);

    String getTableName(Table table);
}
