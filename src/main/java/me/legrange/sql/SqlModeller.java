package me.legrange.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static java.lang.String.format;

public final class SqlModeller {

    private final Supplier<Connection> supplier;
    private final Driver driver;

    /**
     * Create a new modeller with the given connection supplier and driver.
     *
     * @param supplier The connection supplier
     * @param driver   The driver
     */
    SqlModeller(Supplier<Connection> supplier, Driver driver) {
        this.supplier = supplier;
        this.driver = driver;
    }

    /**
     * Compare two columns by their typing. Returns true if they are essentially the same.
     *
     * @param one   One column
     * @param other The other column
     * @return True if the same
     */
    public boolean typesAreCompatible(Column one, Column other) {
        return driver.typesAreCompatible(one, other);
    }

    /**
     * Read a database from SQL and return a model for it.
     *
     * @param name The name of the database to read
     * @return The model
     * @throws SqlManagerException Thrown if there is a problem reading the model
     */
    public Database readDatabase(String name) throws SqlManagerException {
        SqlDatabase database = new SqlDatabase(name);
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(null, null, null, null)) {
                while (tables.next()) {
                    database.addTable(readTable(database, tables.getString("TABLE_NAME")));
                }
            }
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error scanning database '%s' (%s)", name, ex.getMessage()));
        }
        return database;
    }

    /**
     * Read a table from SQL and return a model for it.
     *
     * @param database The database for the table
     * @param name     The name of the table
     * @return The table model
     * @throws SqlManagerException Thrown if there is a problem reading the model
     */
    public Table readTable(Database database, String name) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            SqlTable table = new SqlTable(database, name);
            Map<String, SqlColumn> sqlColumns = new HashMap<>();
            try (ResultSet columns = dbm.getColumns(database.getName(), null, table.getName(), "%")) {
                while (columns.next()) {
                    SqlColumn column = getColumnFromResultSet(table, columns);
                    sqlColumns.put(column.getName(), column);
                }
            }
            Set<String> keyNames = new HashSet<>();
            try (ResultSet keys = dbm.getPrimaryKeys(database.getName(), null, table.getName())) {
                while (keys.next()) {
                    SqlColumn column = sqlColumns.get(keys.getString("COLUMN_NAME"));
                    String pkName = keys.getString("PK_NAME");
                    if (column == null) {
                        throw new SqlManagerException(format("Cannot find column '%s' in table '%s' yet it is a primary key", keys.getString("COLUMN_NAME"), table.getName()));
                    }
                    keyNames.add(pkName);
                    column.setKey(true);
                }
            }
            for (Column column : sqlColumns.values()) {
                table.addColumn(column);
            }
            Map<String, SqlIndex> idxMap = new HashMap<>();
            try (ResultSet indexes = dbm.getIndexInfo(database.getName(), null, table.getName(), false, false)) {
                while (indexes.next()) {
                    String index_name = indexes.getString("INDEX_NAME");
                    String column_name = indexes.getString("COLUMN_NAME");
                    boolean non_unique = indexes.getBoolean("NON_UNIQUE");
                    SqlIndex sqlIndex;
                    if (idxMap.containsKey(index_name)) {
                        sqlIndex = idxMap.get(index_name);
                    } else {
                        sqlIndex = new SqlIndex(table, index_name, !non_unique);
                        idxMap.put(index_name, sqlIndex);
                    }
                    sqlIndex.addColunm(table.getColumn(column_name));
                }
            }
            for (Index index : idxMap.values()) {
                if (!keyNames.contains(index.getName())) {
                    table.addIndex(index);
                }
            }
            return table;
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error scanning table '%s' (%s)", name, ex.getMessage()));
        }
    }

    /**
     * Deterime if a table exists in SQL
     *
     * @param table The table
     * @return Does it exist?
     * @throws SqlManagerException Thrown if there is a problem
     */
    public boolean tableExists(Table table) throws SqlManagerException {
        return tableExists(table.getDatabase(), table.getName());
    }

    /**
     * Create a table based on a table model.
     *
     * @param table The table model
     * @throws SqlManagerException Thrown if there is a problem creating the table
     */
    public void createTable(Table table) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeCreateTableQuery(table));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error creating table '%s' (%s)", table.getName(), ex.getMessage()));
        }
    }


    /**
     * Delete a table from SQL
     *
     * @param table The table model
     * @throws SqlManagerException Thrown if there is a problem deleting the table
     */
    public void deleteTable(Table table) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeDeleteTableQuery(table));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error deleting table '%s' (%s)", table.getName(), ex.getMessage()));
        }
    }

    /**
     * Add a column to a table.
     *
     * @param column The column to add
     * @throws SqlManagerException Thrown if there is a problem adding the column
     */
    public void addColumn(Column column) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeAddColumnQuery(column));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding column '%s' to table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Rename a column.
     *
     * @param current The current column
     * @param changed The changed column
     * @throws SqlManagerException Thrown if there is a problem reaming the column
     */
    public void renameColumn(Column current, Column changed) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeRenameColumnQuery(current, changed));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error renaming column '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }


    /**
     * Delete a column from SQL
     *
     * @param column The column to delete
     * @throws SqlManagerException Thrown if there is a problem deleting the column
     */
    public void deleteColumn(Column column) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeDeleteColumnQuery(column));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error deleting column '%s' from table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Modify a column in SQL.
     *
     * @param current The current column
     * @throws SqlManagerException Thrown if there is a problem modifying the model
     */
    public void modifyColumn(Column current) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeModifyColumnQuery(current));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error modifying column '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Add an index to a SQL table.
     *
     * @param index The index to add
     * @throws SqlManagerException
     */
    public void addIndex(Index index) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeAddIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Rename an index on a SQL table.
     *
     * @param current The index to modify
     * @param changed The changed index
     * @throws SqlManagerException
     */
    public void renameIndex(Index current, Index changed) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeRenameIndexQuery(current, changed));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error renaming index '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }

    }

    /**
     * Modify an index on a SQL table
     *
     * @param index The index to modify
     * @throws SqlManagerException
     */
    public void modifyIndex(Index index) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            if (driver.supportsAlterIndex()) {
                stmt.executeUpdate(driver.makeModifyIndexQuery(index));
            } else {
                removeIndex(index);
                addIndex(index);
            }
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error modifying index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Remove an index from a SQL table.
     *
     * @param index The index to remove
     * @throws SqlManagerException
     */
    public void removeIndex(Index index) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(driver.makeRemoveIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error removing index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Deterime if a table exists in a database in SQL
     *
     * @param db        The database
     * @param tableName The table name
     * @return Does it exist?
     * @throws SqlManagerException Thrown if there is a problem
     */
    private boolean tableExists(Database db, String tableName) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(databaseName(db), null, tableName, null)) {
                return tables.next();
            }
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error checking table '%s' (%s)", tableName, ex.getMessage()));
        }
    }

    /**
     * Read a column model from a SQL result set.
     *
     * @param table The table for the column
     * @param rs    The result set
     * @return The column mode
     * @throws SqlManagerException
     */
    private SqlColumn getColumnFromResultSet(Table table, ResultSet rs) throws SqlManagerException {
        try {
            JDBCType jdbcType = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
            Optional<Integer> size;
            switch (jdbcType) {
                case CHAR:
                case VARCHAR:
                case LONGVARCHAR:
                case BIT:
                    size = Optional.of(rs.getInt("COLUMN_SIZE"));
                    break;
                default:
                    size = Optional.empty();
            }
            boolean nullable = rs.getString("IS_NULLABLE").equals("YES");
            boolean autoIncrement = rs.getString("IS_AUTOINCREMENT").equals("YES");
            String colunmName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");

            if (driver.isEnumColumn(colunmName, jdbcType, typeName)) {
                Set<String> values = Collections.emptySet();
                String query = driver.makeReadEnumQuery(new SqlEnumColumn(table, colunmName, nullable, values));
                try (Connection con = con(); Statement stmt = con.createStatement(); ResultSet ers = stmt.executeQuery(query)) {
                    if (ers.next()) {
                        values = driver.extractEnumValues(ers.getString(1));
                    }
                }
                return new SqlEnumColumn(table, colunmName, nullable, values);
            } else if (driver.isSetColumn(colunmName, jdbcType, typeName)) {
                Set<String> values = Collections.emptySet();
                String query = driver.makeReadSetQuery(new SqlSetColumn(table, colunmName, nullable, values));
                try (Connection con = con(); Statement stmt = con.createStatement(); ResultSet ers = stmt.executeQuery(query)) {
                    if (ers.next()) {
                        values = driver.extractSetValues(ers.getString(1));
                    }
                }
                return new SqlSetColumn(table, colunmName, nullable, values);
            } else if (driver.isStringColumn(colunmName, jdbcType, typeName)) {
                return new SqlStringColumn(table, colunmName, jdbcType, nullable, size.get());
            }
            switch (jdbcType) {
                case BIT:
                    return new SqlBitColumn(table, colunmName, jdbcType, nullable, autoIncrement, size.get());
                case BOOLEAN:
                    return new SqlBooleanColumn(table, colunmName, jdbcType, nullable, autoIncrement);
            }
            return new SqlColumn(table,
                    colunmName,
                    jdbcType,
                    nullable,
                    autoIncrement);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error reading SQL column information (%s)", ex.getMessage()), ex);
        }
    }

    private String databaseName(Database database) {
        return driver.getDatabaseName(database);
    }

    private Connection con() {
        return supplier.get();
    }

}
