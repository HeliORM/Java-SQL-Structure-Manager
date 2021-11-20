package me.legrange.sql;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

@TestMethodOrder(OrderAnnotation.class)
public class TestCRUD extends AbstractSqlTest {

    @Test
    @Order(10)
    public void createTable() throws SqlManagerException {
        table.addColumn(new TestColumn(table, "id", JDBCType.INTEGER, false, true, true));
        table.addColumn(new TestStringColumn(table, "name", JDBCType.VARCHAR, 42));
        table.addColumn(new TestColumn(table, "age", JDBCType.SMALLINT));
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
    public void addStringColumn() throws SqlManagerException {
        TestColumn email = new TestStringColumn(table, "email", JDBCType.VARCHAR, 128);
        table.addColumn(new TestStringColumn(table, "email", JDBCType.VARCHAR, 128));
        modeller.addColumn(email);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(21)
    public void addBooleanColumn() throws SqlManagerException {
        TestColumn sex = new TestBooleanColumn(table, "sex");
        table.addColumn(sex);
        modeller.addColumn(sex);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(22)
    public void renameColumn() throws SqlManagerException {
        Column fullName = new TestStringColumn(table, "fullName", JDBCType.VARCHAR, 42);
        modeller.renameColumn(table.getColumn("name"), fullName);
        table.deleteColumn(table.getColumn("name"));
        table.addColumn(fullName);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(30)
    public void addLongTextColumn() throws SqlManagerException {
        TestColumn notes = new TestStringColumn(table, "notes", JDBCType.LONGVARCHAR, 10000);
        TestColumn lnotes = new TestStringColumn(table, "longNotes", JDBCType.LONGVARCHAR, 16 * 1024 * 1024);
        table.addColumn(notes);
        table.addColumn(lnotes);
        modeller.addColumn(notes);
        modeller.addColumn(lnotes);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(30)
    public void addDecimalColumn() throws SqlManagerException {
        TestColumn amount = new TestDecimalColumn(table, "amount", 18, 2);
        table.addColumn(amount);
        modeller.addColumn(amount);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(40)
    public void addEnumColumn() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type", true, new HashSet<>(Arrays.asList("APE", "BEAST")));
        table.addColumn(type);
        modeller.addColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(50)
    public void addEnumValue() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type", true, new HashSet<>(Arrays.asList("APE", "BEAST", "COW")));
        table.addColumn(type);
        modeller.modifyColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(60)
    public void removeEnumValue() throws SqlManagerException {
        TestColumn type = new TestEnumColumn(table, "type", true, new HashSet<>(Arrays.asList("APE", "COW")));
        table.addColumn(type);
        modeller.modifyColumn(type);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(61)
    public void addSetColumn() throws SqlManagerException {
        if (modeller.supportsSet()) {
            TestColumn col = new TestSetColumn(table, "selection", true, new HashSet<>(Arrays.asList("BREAKFAST", "LUNCH", "DINNER")));
            table.addColumn(col);
            modeller.addColumn(col);
            Table loaded = modeller.readTable(db, "Person");
            assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
        }
    }

    @Test
    @Order(62)
    public void addSetValue() throws SqlManagerException {
        if (modeller.supportsSet()) {
            TestColumn col = new TestSetColumn(table, "selection", true, new HashSet<>(Arrays.asList("BREAKFAST", "2ND BREAKFAST", "LUNCH", "DINNER")));
            table.addColumn(col);
            modeller.modifyColumn(col);
            Table loaded = modeller.readTable(db, "Person");
            assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
        }
    }

    @Test
    @Order(63)
    public void removeSetValue() throws SqlManagerException {
        if (modeller.supportsSet()) {
            TestColumn col = new TestEnumColumn(table, "selection", true, new HashSet<>(Arrays.asList("BREAKFAST", "LUNCH", "DINNER")));
            table.addColumn(col);
            modeller.modifyColumn(col);
            Table loaded = modeller.readTable(db, "Person");
            assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
        }
    }

    @Test
    @Order(70)
    public void deleteColumn() throws SqlManagerException {
        TestColumn email = new TestStringColumn(table, "email", JDBCType.VARCHAR, 128);
        TestColumn notes = new TestStringColumn(table, "notes", JDBCType.LONGVARCHAR, 1000);
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
        TestColumn name = new TestStringColumn(table, "fullName", JDBCType.VARCHAR, true, false, 64);
        table.addColumn(name);
        modeller.modifyColumn(name);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(90)
    public void modifyColumnTypeSmallIntBigInt() throws SqlManagerException {
        TestColumn age = new TestColumn(table, "age", JDBCType.BIGINT, false, false, false);
        table.addColumn(age);
        Table loaded = modeller.readTable(db, "Person");
        assertFalse(isSameTable(loaded, table), "Table we modified must not be the same as the one loaded");
        modeller.modifyColumn(age);
        loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(100)
    public void modifyColumnType() throws SqlManagerException {
        TestColumn sex = new TestBooleanColumn(table, "sex");
        table.addColumn(sex);
        modeller.modifyColumn(sex);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(110)
    public void addSingleColumnIndex() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index0", true);
        index.addColumn(table.getColumn("fullName"));
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(111)
    public void addMultiColumnIndex() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index1", true);
        index.addColumn(table.getColumn("fullName"));
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.addIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(112)
    public void changeIndexUninqueness() throws SqlManagerException {
        TestIndex index = new TestIndex(table, "index1", false);
        index.addColumn(table.getColumn("fullName"));
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.modifyIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }


    @Test
    @Order(120)
    public void addColumnToIndex() throws SqlManagerException {
        TestIndex index = (TestIndex) table.getIndex("index0");
        index.addColumn(table.getColumn("age"));
        table.addIndex(index);
        modeller.modifyIndex(index);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(121)
    public void renameIndex() throws SqlManagerException {
        TestIndex index = (TestIndex) table.getIndex("index0");
        TestIndex index1 = new TestIndex(table, "index7", index.isUnique());
        for (Column column : index.getColumns()) {
            index1.addColumn(column);
        }
        modeller.renameIndex(index, index1);
        table.removeIndex(index);
        table.addIndex(index1);
        Table loaded = modeller.readTable(db, "Person");
        assertTrue(isSameTable(loaded, table), "Table we modified must be the same as the one loaded");
    }

    @Test
    @Order(130)
    public void removeIndex() throws SqlManagerException {
        Index index = table.getIndex("index1");
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
