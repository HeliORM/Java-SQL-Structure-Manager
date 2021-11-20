package com.heliorm.sql;

/** Column representing a decimal number */
public interface DecimalColumn extends Column {

    /** Get the precision of the number.
     *
     * @return The precision
     */
    int getPrecision();

    /** Get the scale of the number.
     *
     * @return The scale
     */
    int getScale();

}
