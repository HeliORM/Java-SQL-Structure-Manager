package me.legrange.sql;

import me.legrange.sql.driver.MySqlDriver;
import me.legrange.sql.driver.PostgreSql;

import static java.lang.String.format;

public interface Driver {

    /**
     * Make a SQL query to create a table.
     *
     * @param table The table
     * @return The SQL query
     */
    String makeCreateTableQuery(Table table);

    /**
     * Make a SQL query to remove a table.
     *
     * @param table The table
     * @return The SQL query
     */
    String makeRemoveTableQuery(Table table);

    /**
     * Make a SQL query to delete a table.
     *
     * @param table The table
     * @return The SQL query
     */
    String makeDeleteTableQuery(Table table);

    /**
     * Make a SQL query to remove an index from a table
     *
     * @param index The index model
     * @return The SQL query
     */
    String makeRemoveIndexQuery(Index index);

    /**
     * Make a SQL query to rename an index on a table
     *
     * @param current The current index model
     * @param changed The changed index model
     * @return The SQL query
     */
    String makeRenameIndexQuery(Index current, Index changed);


    String makeModifyIndexQuery(Index index);

    /**
     * Make a SQL query to rename a column in a table
     *
     * @param column  The column model
     * @param changed The changed column model
     * @return The SQL query
     */
    String makeRenameColumnQuery(Column column, Column changed);

    /**
     * Make a SQL query to delete a column from a table
     *
     * @param column The column model
     * @return The SQL query
     */
    String makeDeleteColumnQuery(Column column);

    /**
     * Make a SQL query to modify a column in a table
     *
     * @param column The column model
     * @return The SQL query
     */
    String makeModifyColumnQuery(Column column);

    /**
     * Make a SQL query to add a column to a table
     *
     * @param column The column model
     * @return The SQL query
     */
     String makeAddColumnQuery(Column column);

    /**
     * Make a SQL query to add an index to a table
     *
     * @param index The index model
     * @return The SQL query
     */
     String makeAddIndexQuery(Index index) ;


    //// -- old

    String getDatabaseName(Database database);

    String getTableName(Table table);

    String getCreateType(Column column);

    String getColumnName(Column column);

    String getIndexName(Index index);

    boolean supportsAlterIndex();

    static Driver mysql() {
        return new MySqlDriver();
    }

    static Driver posgresql() {
        return new PostgreSql();
    }

}
