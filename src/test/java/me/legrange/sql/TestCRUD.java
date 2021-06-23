package me.legrange.sql;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.JDBCType;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
public class TestCRUD extends AbstractSqlTest {


    @Test
    @Order(1)
    public void createTable() throws SqlManagerException {
        table.addColumn(new TestColumn(table, "id", JDBCType.INTEGER, Integer.class, Optional.empty(), false, true, true));
        table.addColumn(new TestColumn(table, "name", JDBCType.VARCHAR, String.class, Optional.of(42), false, false, false));
        table.addColumn(new TestColumn(table, "age", JDBCType.SMALLINT, Integer.class));
        table.addColumn(new TestColumn(table, "sex", JDBCType.BIT, Boolean.class, Optional.empty(), false, false, false));
        if (modeller.tableExists(table)) {
            say("Removing table %s", table.getName());
            modeller.deleteTable(table);
        }
        modeller.createTable(table);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we created must be the same as the one loaded ");
    }

    @Test
    @Order(2)
    public void addColumnToTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR, String.class, Optional.of(128), false, false, false);
        table.addColumn(new TestColumn(table, "email", JDBCType.VARCHAR, String.class, Optional.of(128), false, false, false));
        modeller.addColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded ");
    }

    @Test
    @Order(3)
    public void deleteColumnFromTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR, String.class, Optional.of(128), false, false, false);
        table.deleteColumn(email);
        modeller.deleteColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded ");
    }

    @Test
    @Order(4)
    public void alterColumnOnTable() throws SqlManagerException {
        TestColumn name = new TestColumn(table, "name", JDBCType.VARCHAR, String.class, Optional.of(64), true, false, false);
        table.addColumn(name);
        modeller.modifyColumn(name);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded ");
    }

    @Test
    @Order(5)
    public void addIndexToTable() throws SqlManagerException {
        Column email = table.getColumns().stream()
                .filter(column ->  column.getName().equals("name"))
                .findFirst().get();
        Set<Column> columns = new HashSet<>();
        columns.add(email);
        TestIndex index = new TestIndex(table, "index0", true, columns);
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded ");
    }

    private boolean isSameTable(Table one, TestTable other) {
        return one.getDatabase().getName().equals(other.getDatabase().getName())
                && isSameColumns(one.getColumns(), other.getColumns());
//                && isSameColumns(one.getIndexes(), other.getIndexes());
    }

    private boolean isSameColumns(Set<Column> one, Set<Column> other) {
        if (one.size() != other.size()) {
            return false;
        }
        Map<String, Column> oneMap = one.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Column> otherMap = other.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        for (String name : oneMap.keySet()) {
            if (!otherMap.containsKey(name)) {
                return false;
            }
            if (!isSameColumn(oneMap.get(name), otherMap.get(name))) {
                return false;
            }
        }
        return true;
    }

    private boolean isSameColumn(Column one, Column other) {
        boolean same = one.isAutoIncrement() == other.isAutoIncrement()
                && one.isNullable() == other.isNullable()
                && one.isKey() == other.isKey()
                && one.getLength().equals(other.getLength())
                && one.getName().equals(other.getName())
                && one.getJavaType().equals(other.getJavaType())
                && one.getJdbcType().equals(other.getJdbcType());
        if (!same) {
            System.out.println("" + one + "\nvs\n" + other);
        }
        return same;
    }

}
