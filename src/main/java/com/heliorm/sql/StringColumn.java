package com.heliorm.sql;

/** Implementation of string column that is populated by reading from SQL
 *
 */
public interface StringColumn extends Column {

    /** Return the length of the string column.
     *
     * @return The length
     */
    int getLength();

}
