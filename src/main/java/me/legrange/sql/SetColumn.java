package me.legrange.sql;

import java.util.Set;

public interface SetColumn extends Column{

    Set<String> getSetValues();

}
