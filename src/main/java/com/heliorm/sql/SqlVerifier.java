package com.heliorm.sql;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Tool for verifying if a user supplied SQL data structure is the same as the one in a database.
 *
 */
public final class SqlVerifier {

    private final SqlModeller modeller;
    private boolean deleteMissingColumns = false;

    /** Create a new verifier for the supplied SQL modeller.
     *
     * @param modeller The modeller to use
     * @return The verifier
     */
    public static SqlVerifier forModeller(SqlModeller modeller) {
        return new SqlVerifier(modeller);
    }

    /** Setup verifier to delete missing columns from a database table
     *
     * @param delete True if it must delete.
     */
    void setDeleteMissingColumns(boolean delete) {
        this.deleteMissingColumns = delete;
    }

    /** Verify that a table in a SQL database is the same as the abstraction supplied, and change the database
     * to conform if not.
     *
     * @param table The table
     * @return The changes made to synchronize the table.
     * @throws SqlModellerException
     */
    public List<Action> synchronizeDatabaseTable(Table table) throws SqlModellerException {
        if (!modeller.tableExists(table)) {
            modeller.createTable(table);
            return Collections.singletonList(Action.createTable(table));
        } else {
            List<Action> actions = new ArrayList<>();
            actions.addAll(synchronizeColumns(table));
            actions.addAll(synchronizeIndexes(table));
            return actions;
        }
    }

    private List<Action> synchronizeColumns(Table table) throws SqlModellerException {
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
                if (!isSame(tableColumn,sqlColumn)) {
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

    private List<Action> synchronizeIndexes(Table table) throws SqlModellerException {
        Table sqlTable = modeller.readTable(table.getDatabase(), table.getName());
        Map<String, Index> tableIndexes = table.getIndexes().stream()
                .collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Index> sqlIndexes = sqlTable.getIndexes().stream()
                .collect(Collectors.toMap(col -> col.getName(), col -> col));
        List<Action> actions = new ArrayList<>();
        for (String name : tableIndexes.keySet()) {
            Index tableIndex = tableIndexes.get(name);
            if (!sqlIndexes.containsKey(name)) {
                modeller.addIndex(tableIndex);
                actions.add(Action.addIndex(tableIndex));
            } else {
                Index sqlIndex = sqlIndexes.get(name);
                if (!isSame(tableIndex, sqlIndex)) {
                    modeller.modifyIndex(tableIndex);
                    actions.add(Action.modifyIndex(tableIndex));
                }
            }
        }
        for (String name : sqlIndexes.keySet()) {
            Index sqlIndex  = sqlIndexes.get(name);
            if (!tableIndexes.containsKey(name)) {
                if (deleteMissingColumns) {
                    modeller.removeIndex(sqlIndex);
                    actions.add(Action.deleteIndex(sqlIndex));
                }
            }
        }
        return actions;
    }

    private boolean isSame(Column one, Column other) {
        return one.isAutoIncrement() == other.isAutoIncrement()
                && one.isNullable() == other.isNullable()
                && one.isKey() == other.isKey()
                && one.getName().equals(other.getName())
                && ((one.getDefault() != null && other.getDefault() != null && one.getDefault().equals(other.getDefault()))
                || (one.getDefault() == null && other.getDefault() == null))
                && modeller.typesAreCompatible(one,other);
    }

    private boolean isSame(Index one, Index other) {
        boolean same = one.getName().equals(other.getName())
                && (one.isUnique() == other.isUnique());
        if (same) {
            return isSame(one.getColumns(), other.getColumns());
        }
        return false;
    }

    private boolean isSame(Set<Column> one, Set<Column> other) {
        if (one.size() != other.size()) {
            return false;
        }
        Map<String, Column> oneMap = one.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Column> otherMap = other.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        for (String name : oneMap.keySet()) {
            if (!otherMap.containsKey(name)) {
                return false;
            }
            if (!isSame(oneMap.get(name), otherMap.get(name))) {
                return false;
            }
        }
        return true;
    }

    private SqlVerifier(SqlModeller modeller) {
        this.modeller = modeller;
    }

}
