package com.heliorm.sql;

/** Column representing a bitset */
public interface BitColumn extends Column {

    /** Get the number of bits in the column.
     *
     * @return The bits
     */
    int getBits();

}
