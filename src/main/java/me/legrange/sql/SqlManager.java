package me.legrange.sql;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SqlManager {

    private final Supplier<Connection> supplier;
    private final Driver driver;
    private final SqlModeller modeller;
    private boolean deleteMissingColumns = false;

    SqlManager(Supplier<Connection> supplier, Driver driver) {
        this.supplier = supplier;
        this.driver = driver;
        this.modeller = new SqlModeller(supplier, driver);
    }

    void setDeleteMissingColumns(boolean delete) {
        this.deleteMissingColumns = delete;
    }

    public List<Action> verifyTable(Table table) throws SqlManagerException {
        if (!modeller.tableExists(table)) {
            modeller.createTable(table);
            return Collections.singletonList(Action.createTable(table));
        } else {
            return verifyStructure(table);
        }
    }

    private List<Action> verifyStructure(Table table) throws SqlManagerException {
        Table sqlTable = modeller.readTable(table.getDatabase(), table.getName());
            Map<String, Column> tableColumns = table.getColumns().stream()
                    .collect(Collectors.toMap(col -> col.getName(), col -> col));
            Map<String, Column> sqlColumns = sqlTable.getColumns().stream()
                    .collect(Collectors.toMap(col -> col.getName(), col -> col));
            List<Action> actions = new ArrayList<>();
            for (String name : tableColumns.keySet()) {
                Column tableColumn = tableColumns.get(name);
                if (!sqlColumns.containsKey(name)) {
                    modeller.addColumn(tableColumn);
                    actions.add(Action.addColumn(tableColumn));
                } else {
                    Column sqlColumn = sqlColumns.get(name);
                    if (!tableColumn.equals(sqlColumn)) {
                        modeller.modifyColumn(tableColumn);
                        actions.add(Action.modifyColumn(tableColumn));
                    }
                }
            }
            for (String name : sqlColumns.keySet()) {
                Column sqlColumn = sqlColumns.get(name);
                if (!tableColumns.containsKey(name)) {
                    if (deleteMissingColumns) {
                        modeller.deleteColumn(sqlColumn);
                        actions.add(Action.deleteColumn(sqlColumn));
                    }
                }
            }
            return actions;
    }

}
