package me.legrange.sql;

import me.legrange.sql.driver.MySqlDriver;

public interface Driver {

    String getDatabaseName(Database database);

    String getTableName(Table table);

    String getCreateType(Column column);

    String getColumnName(Column column);

    String getIndexName(Index index);

    boolean supportsAlterIndex();

    static Driver mysql() {
        return new MySqlDriver();
    }
}
