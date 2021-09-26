package me.legrange.sql;

import java.util.Set;

/** Abstraction representing a SQL database
 *
 */
public interface Database {

    /** Return the name of the database.
     *
     * @return The name
      */
    String getName();

    /** Return the tables in this database.
     *
     * @return The tables
     */
    Set<Table> getTables();

}
