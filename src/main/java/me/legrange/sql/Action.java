package me.legrange.sql;

import static java.lang.String.format;

public final class Action {

    public enum Type {
        CREATE_TABLE;
    }

    private final Type type;
    private final String message;

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