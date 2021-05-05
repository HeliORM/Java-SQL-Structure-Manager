package me.legrange.sql;

import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCreate extends AbstractSqlTest {


    @Test
    public void createTable() throws SqlManagerException {
        TestDatabase db = new TestDatabase("neutral");
        TestTable table = new TestTable(db, "Person");
        table.addColumn(new TestColumn(table, "id", JDBCType.INTEGER, Integer.class, Optional.empty(), false, true, true));
        table.addColumn(new TestColumn(table, "name", JDBCType.VARCHAR, String.class, Optional.of(42), false, false, false));
        table.addColumn(new TestColumn(table, "age", JDBCType.SMALLINT, Integer.class));
        table.addColumn(new TestColumn(table, "sex", JDBCType.BIT, Boolean.class, Optional.empty(), false, false, false));
        List<Action> actions = manager.verifyTable(table);
        Table loaded = manager.scanTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we created is same as the one loaded ");
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
        boolean same =  one.isAutoIncrement() == other.isAutoIncrement()
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
