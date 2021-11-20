package me.legrange.sql;

import java.util.Set;


/** An abstraction for a database table.
 *
 */
public interface Table {

    /** Get the database in which this table exists.
     *
     * @return The database
     */
    Database getDatabase();

    /** Get the name of this table (relative to the datbase)
     *
     * @return The table name
     */
    String getName();

    /** Get the columns in this table
     *
     * @return The columns
     */
    Set<Column> getColumns();

    /** Get the column with the given name
     *
     * @param name The name of the column
     * @return The column or null if no column with that name exists
     */
    Column getColumn(String name);

    /** Get the indexes in this table
     *
     * @return The indexes
     */
    Set<Index> getIndexes();

    /** Get the index with the given name
     *
     * @param name The name of the index
     * @return The index or null if no index with that name exists
     */
    Index getIndex(String name);

}
