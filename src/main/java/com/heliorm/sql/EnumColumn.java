package com.heliorm.sql;

import java.util.Set;

/** Column representing an enum */
public interface EnumColumn extends Column{

    /** Get the allowed enum values.
     *
     * @return The values
     */
    Set<String> getEnumValues();

}
