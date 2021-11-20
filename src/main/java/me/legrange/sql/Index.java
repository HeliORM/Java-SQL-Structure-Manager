package me.legrange.sql;

import java.util.Set;

/** Abstraction representing a SQL table index
 *
 */
public interface Index {

    /** Get the name of the index
     *
     * @return The name
     */
    String getName();

    /** Get the table to which the index applies
     *
     * @return The table
     */
    Table getTable();

    /** Get the columns in the table making up the index.
     *
     * @return The columns
     */
    Set<Column> getColumns();

    /** Is the index unique?
     *
     * @return True if so
     */
    boolean isUnique();

}
