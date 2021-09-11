package me.legrange.sql;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
public class TestCRUD extends AbstractSqlTest {

    @Test
    @Order(10)
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
    @Order(20)
    public void addColumnToTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false);
        table.addColumn(new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false));
        modeller.addColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(30)
    public void addLongTextColumnToTable() throws SqlManagerException {
        TestColumn notes = new TestColumn(table, "notes", JDBCType.LONGVARCHAR, Optional.of(10000), false, false, false);
        TestColumn lnotes = new TestColumn(table, "longNotes", JDBCType.LONGVARCHAR, Optional.of(16*1024*1024), false, false, false);
        table.addColumn(notes);
        table.addColumn(lnotes);
        modeller.addColumn(notes);
        modeller.addColumn(lnotes);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(30)
    public void addDecimalColumnToTable() throws SqlManagerException {
        TestColumn amount = new TestColumn(table, "amount", JDBCType.DECIMAL, Optional.of(18), false, false, false);
        table.addColumn(amount);
        modeller.addColumn(amount);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(40)
    public void addEnumColumnToTable() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type",true,  new HashSet<>(Arrays.asList("APE", "BEAST")));
        table.addColumn(type);
        modeller.addColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(50)
    public void addEnumValue() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type",true,  new HashSet<>(Arrays.asList("APE", "BEAST", "COW")));
        table.addColumn(type);
        modeller.modifyColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(60)
    public void removeEnumValue() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type",true,  new HashSet<>(Arrays.asList("APE", "COW")));
        table.addColumn(type);
        modeller.modifyColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(70)
    public void deleteColumnFromTable() throws SqlManagerException {
        TestColumn email = new TestColumn(table, "email", JDBCType.VARCHAR,  Optional.of(128), false, false, false);
        TestColumn notes = new TestColumn(table, "notes", JDBCType.LONGVARCHAR, Optional.of(10000), false, false, false);
        table.deleteColumn(email);
        table.deleteColumn(notes);
        modeller.deleteColumn(email);
        modeller.deleteColumn(notes);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(80)
    public void modifyColumnLength() throws SqlManagerException {
        TestColumn name = new TestColumn(table, "name", JDBCType.VARCHAR, Optional.of(64), true, false, false);
        table.addColumn(name);
        modeller.modifyColumn(name);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(90)
    public void modifyColumnTypeSmallIntBigInt() throws SqlManagerException {
        TestColumn age = new TestColumn(table, "age", JDBCType.BIGINT, Optional.empty(), false, false, false);
        table.addColumn(age);
        modeller.modifyColumn(age);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(100)
    public void modifyColumnType() throws SqlManagerException {
        TestColumn sex = new TestColumn(table, "sex", JDBCType.BOOLEAN, Optional.empty(), false, false, false);
        table.addColumn(sex);
        modeller.modifyColumn(sex);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(110)
    public void addIndexSingleColumnTable() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index0", true);
        index.addColumn(table.getColumn("name"));
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(111)
    public void addMultiColumnIndexToTable() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index1", true);
        index.addColumn(table.getColumn("name"));
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(120)
    public void modifyIndexOnTable() throws SqlManagerException {
        TestIndex index = (TestIndex) table.getIndex("index0");
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.modifyIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(130)
    public void removeIndexFromTable() throws SqlManagerException {
        Index index =table.getIndex("index0");
        table.removeIndex(index);
        modeller.removeIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(140)
    public void deleteTable() throws SqlManagerException {
        modeller.deleteTable(table);
        assertTrue(!modeller.tableExists(table), "Table must not exist any more");
    }

}
