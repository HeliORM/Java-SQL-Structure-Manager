package me.legrange.sql;

import java.util.Optional;
import java.util.Set;

public interface EnumColumn extends Column{

    Set<String> getEnumValues();

}
