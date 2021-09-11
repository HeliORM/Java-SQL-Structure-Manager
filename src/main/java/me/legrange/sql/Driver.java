package me.legrange.sql;

import me.legrange.sql.driver.MySqlDriver;
import me.legrange.sql.driver.PostgreSql;

import java.sql.JDBCType;
import java.util.Set;

public interface Driver {

    /**
     * Make a SQL query to create a table.
     *
     * @param table The table
     * @return The SQL query
     */
    String makeCreateTableQuery(Table table);

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

    /**
     * Make a SQL query to modify an index to a table
     *
     * @param index The index model
     * @return The SQL query
     */
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
    String makeAddIndexQuery(Index index);

    /** Compare two columns to see if they have the same effective type according to
     * a specific database implementation.
     *
     * @param one
     * @param other
     * @return
     */
    boolean typesAreCompatible(Column one, Column other);

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

    boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName);

    String makeReadEnumQuery(EnumColumn column);

    Set<String> extractEnumValues(String text);

    boolean isSetColumn(String colunmName, JDBCType jdbcType, String typeName);

    String makeReadSetQuery(SetColumn sqlSetColumn);

    Set<String> extractSetValues(String string);
}
