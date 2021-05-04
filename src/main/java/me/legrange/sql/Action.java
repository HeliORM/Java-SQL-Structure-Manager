package me.legrange.sql;

import static java.lang.String.format;

public final class Action {

    public enum Type {
        CREATE_TABLE,
        ADD_COLUMN,
        DELETE_COLUMN,
        MODIFY_COLUMN,
        RENAME_COLUMN;
    }

    private final Type type;
    private final String message;

    static Action modifyColumn(Column column) {
        return new Action(Type.MODIFY_COLUMN, format("Modified column %s in table %s in database %s",
                column.getName(),
                column.getTable().getName(),
                column.getTable().getDatabase().getName()));
    }

    static Action renameColumn(Column current, Column changed) {
        return new Action(Type.RENAME_COLUMN, format("Renamed column %s to %s in table %s in database %s",
                current.getName(),
                changed.getName(),
                current.getTable().getName(),
                current.getTable().getDatabase().getName()));
    }

    static Action deleteColumn(Column column) {
        return new Action(Type.DELETE_COLUMN, format("Deleted column %s from table %s in database %s",
                column.getName(),
                column.getTable().getName(),
                column.getTable().getDatabase().getName()));
    }


    static Action addColumn(Column column) {
        return new Action(Type.ADD_COLUMN, format("Added column %s to table %s in database %s",
                column.getName(),
                column.getTable().getName(),
                column.getTable().getDatabase().getName()));
    }

    static Action createTable(Table table) {
        return new Action(Type.CREATE_TABLE,
                format("Created table %s in database %s",
                        table.getName(), table.getDatabase().getName()));
    }

    private Action(Type type, String message) {
        this.type = type;
        this.message = message;
    }

    public Type getType() {
        return type;
    }

    public String getMessage() {
        return message;
    }
}