package me.legrange.sql;

public class SqlManagerException extends Exception {

    public SqlManagerException(String message) {
        super(message);
    }

    public SqlManagerException(String message, Throwable cause) {
        super(message, cause);
    }
}
