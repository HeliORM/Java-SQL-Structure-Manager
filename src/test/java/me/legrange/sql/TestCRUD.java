package me.legrange.sql;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.JDBCType;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
public class TestCRUD extends AbstractSqlTest {

    @Test
    @Order(1)
    public void createTable() throws SqlManagerException {
        table.addColumn(new TestColumn(table, "id", JDBCType.INTEGER,  Optional.empty(), false, true, true));
        table.addColumn(new TestColumn(table, "name", JDBCType.VARCHAR, Optional.of(42), false, false, false));
        table.addColumn(new TestColumn(table, "age", JDBCType.SMALLINT));
        table.addColumn(new TestColumn(table, "sex", JDBCType.BIT, Optional.empty(), false, false, false));
        if (modeller.tableExists(table)) {
            say("Removing table %s", table.getName());
            modeller.deleteTable(table);
        }
        modeller.createTable(table);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we created must be the same as the one loaded");
    }

    @Test
    @Order(2)
    public void addColumnToTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false);
        table.addColumn(new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false));
        modeller.addColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(3)
    public void deleteColumnFromTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false);
        table.deleteColumn(email);
        modeller.deleteColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(4)
    public void modifyColumnOnTable() throws SqlManagerException {
        TestColumn name = new TestColumn(table, "name", JDBCType.VARCHAR, Optional.of(64), true, false, false);
        table.addColumn(name);
        modeller.modifyColumn(name);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(5)
    public void addIndexToTable() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index0", true);
        index.addColumn(table.getColumn("name"));
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(6)
    public void modifyIndexOnTable() throws SqlManagerException {
        TestIndex index = (TestIndex) table.getIndex("index0");
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.modifyIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(7)
    public void removeIndexFromTable() throws SqlManagerException {
        Index index =table.getIndex("index0");
        table.removeIndex(index);
        modeller.removeIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(8)
    public void deleteTable() throws SqlManagerException {
        modeller.deleteTable(table);
        assertTrue(!modeller.tableExists(table), "Table must not exist any more");
    }

}
