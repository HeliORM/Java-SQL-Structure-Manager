package com.heliorm.sql;

public interface BinaryColumn extends Column {

    /**
     * Return the length of the binary column.
     *
     * @return The length
     */
    int getLength();

}
