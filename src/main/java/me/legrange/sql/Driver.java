package me.legrange.sql;

public interface Driver {

    String getDatabaseName(Database database);

    String getTableName(Table table);

    String getCreateType(Column column);

    String getColumnName(Column column);

    String getIndexName(Index index);
}
