package me.legrange.sql;

public interface StringColumn extends Column {

    /** Return the lenght of the string column.
     *
     * @return The length
     */
    int getLength();

}
