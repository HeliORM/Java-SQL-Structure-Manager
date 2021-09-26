package me.legrange.sql;

public interface DecimalColumn extends Column {

    int getPrecision();
    int getScale();
}
