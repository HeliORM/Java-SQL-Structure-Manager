package me.legrange.sql;

import org.junit.jupiter.api.Test;

import java.sql.JDBCType;
import java.util.List;
import java.util.Optional;

public class TestCreate extends AbstractSqlTest {


    @Test
    public void createTable() throws SqlManagerException {
        TestDatabase db = new TestDatabase("neutral");
        TestTable table = new TestTable(db, "Person");
        table.addColumn(new TestColumn(table, "id", JDBCType.INTEGER, Integer.class, Optional.empty(), false, true, true));
        table.addColumn(new TestColumn(table, "name", JDBCType.VARCHAR, String.class, Optional.of(42), false, false, false));
        table.addColumn(new TestColumn(table, "age", JDBCType.SMALLINT, String.class));
        table.addColumn(new TestColumn(table, "sex", JDBCType.BOOLEAN, Boolean.class, Optional.empty(), false, false, false));
        List<Action> actions = manager.verifyTable(table);
    }
}
