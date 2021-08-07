package me.legrange.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;

import static java.lang.String.format;
import static me.legrange.sql.Types.findJavaType;

public class SqlModeller {

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
            try (ResultSet keys = dbm.getPrimaryKeys(database.getName(), null, table.getName())) {
                while (keys.next()) {
                    SqlColumn column = sqlColumns.get(keys.getString("COLUMN_NAME"));
                    if (column == null) {
                        throw new SqlManagerException(format("Cannot find column '%s' in table '%s' yet it is a primary key", keys.getString("COLUMN_NAME"), table.getName()));
                    }
                    column.setKey(true);
                }
            }
            for (Column column : sqlColumns.values()) {
                table.addColumn(column);
            }
            try (ResultSet indexes = dbm.getIndexInfo(database.getName(), null, table.getName(), true, false)) {
                Map<String, SqlIndex> idxMap = new HashMap<>();
                while (indexes.next()) {
                    String index_name = indexes.getString("INDEX_NAME");
                    String column_name = indexes.getString("COLUMN_NAME");
                    boolean non_unique = indexes.getBoolean("NON_UNIQUE");
                    short type = indexes.getShort("TYPE");
                    int ordinal_position = indexes.getInt("ORDINAL_POSITION");
                    SqlIndex sqlIndex;
                    if (idxMap.containsKey(index_name)) {
                        sqlIndex = idxMap.get(index_name);
                    } else {
                        sqlIndex = new SqlIndex(table, index_name, !non_unique);
                        idxMap.put(index_name, sqlIndex);
                    }
                    sqlIndex.addColunm(table.getColumn(column_name));
                }
                for (Index index : idxMap.values()) {
                    if (!index.getName().equals("PRIMARY"))
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
            stmt.executeUpdate(makeCreateTableQuery(table));
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
            stmt.executeUpdate(makeDeleteTableQuery(table));
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
            stmt.executeUpdate(makeAddColumnQuery(column));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding column '%s' to table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Rename a column.
     *
     * @param current
     * @param changed
     * @throws SqlManagerException Thrown if there is a problem reaming the column
     */
    public void renameColumn(Column current, Column changed) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRenameColumnQuery(current, changed));
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
            stmt.executeUpdate(makeDeleteColumnQuery(column));
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
            stmt.executeUpdate(makeModifyColumnQuery(current));
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
            stmt.executeUpdate(makeAddIndexQuery(index));
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
            stmt.executeUpdate(makeRenameIndexQuery(current, changed));
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
                stmt.executeUpdate(makeModifyIndexQuery(index));
            }
            else {
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
            stmt.executeUpdate(makeRemoveIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error removing index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     *  Remove a table
     *
     * @param table The table to remove
     * @throws SqlManagerException
     */
    public void removeTable(Table  table) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRemoveTableQuery(table));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error removing table '%s' (%s)", table.getName(), ex.getMessage()));
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
     * Make a SQL query to rename a column in a table
     *
     * @param column  The column model
     * @param changed The changed column model
     * @return The SQL query
     */
    private String makeRenameColumnQuery(Column column, Column changed) {
        return format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                tableName(column.getTable()),
                columnName(column),
                columnName(changed));
    }

    /**
     * Make a SQL query to delete a column from a table
     *
     * @param column The column model
     * @return The SQL query
     */
    private String makeDeleteColumnQuery(Column column) {
        return format("ALTER TABLE %s DROP COLUMN %s",
                tableName(column.getTable()),
                columnName(column));
    }

    /**
     * Make a SQL query to modify a column in a table
     *
     * @param column The column model
     * @return The SQL query
     */
    private String makeModifyColumnQuery(Column column) {
        return format("ALTER TABLE %s MODIFY COLUMN %s %s",
                tableName(column.getTable()),
                columnName(column),
                driver.getCreateType(column));
    }

    /**
     * Make a SQL query to add a column to a table
     *
     * @param column The column model
     * @return The SQL query
     */
    private String makeAddColumnQuery(Column column) {
        return format("ALTER TABLE %s ADD COLUMN %s %s",
                tableName(column.getTable()),
                columnName(column),
                driver.getCreateType(column));
    }

    /**
     * Make a SQL query to add an index to a table
     *
     * @param index The index model
     * @return The SQL query
     */
    private String makeAddIndexQuery(Index index) {
        return format("CREATE %sINDEX %s on %s (%s)",
                index.isUnique() ? "UNIQUE " : "",
                indexName(index),
                tableName(index.getTable()),
                index.getColumns().stream()
                        .map(column -> columnName(column))
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }

    /**
     * Make a SQL query to rename an index on a table
     *
     * @param current The current index model
     * @param changed The changed index model
     * @return The SQL query
     */
    private String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER TABLE INDEX %s RENAME INDEX %s to %s",
                tableName(current.getTable()),
                indexName(current),
                indexName(changed));
    }

    private String makeModifyIndexQuery(Index index) {
        return format("ALTER %sINDEX %s ON %s %",
                index.isUnique() ? "UNIQUE " : "",
                indexName(index),
                tableName(index.getTable()),
                index.getColumns().stream()
                        .map(column -> columnName(column))
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }

    /**
     * Make a SQL query to remove an index from a table
     *
     * @param index The index model
     * @return The SQL query
     */
    private String makeRemoveIndexQuery(Index index) {
        return format("DROP INDEX %s on %s",
                indexName(index),
                tableName(index.getTable()));
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
                case VARCHAR:
                case CHAR:
                case LONGVARCHAR:
                    size = Optional.of(rs.getInt("COLUMN_SIZE"));
                    break;
                default:
                    size = Optional.empty();
            }
            boolean nullable = rs.getString("IS_NULLABLE").equals("YES");
            boolean autoIncrement = rs.getString("IS_AUTOINCREMENT").equals("YES");
            return new SqlColumn(table,
                    rs.getString("COLUMN_NAME"),
                    jdbcType,
                    findJavaType(jdbcType),
                    size,
                    nullable,
                    autoIncrement
            );
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error reading SQL column information (%s)", ex.getMessage()));
        }
    }

    /**
     * Make a SQL query to create a table.
     *
     * @param table The table
     * @return The SQL query
     */
    private String makeCreateTableQuery(Table table) {
        StringJoiner body = new StringJoiner(",");
        for (Column column : table.getColumns()) {
            body.add(format("%s %s", columnName(column), driver.getCreateType(column)));
        }
        StringBuilder sql = new StringBuilder();
        sql.append(format("CREATE TABLE %s (", tableName(table)));
        sql.append(body.toString());
        sql.append(")");
        return sql.toString();
    }

    /**
     * Make a SQL query to remove a table.
     *
     * @param table The table
     * @return The SQL query
     */
    private String makeRemoveTableQuery(Table table) {
        return format("DROP TABLE %s", tableName(table));
    }

    /**
     * Make a SQL query to delete a table.
     *
     * @param table The table
     * @return The SQL query
     */
    private String makeDeleteTableQuery(Table table) {
        return format("DROP TABLE %s", tableName(table));
    }

    /**
     * Get the SQL database name for a table
     *
     * @param table The table
     * @return The database name
     */
    private String databaseName(Table table) {
        return databaseName(table.getDatabase());

    }

    private String databaseName(Database database) {
        return driver.getDatabaseName(database);
    }

    /**
     * Get the SQL table name from the given table.
     *
     * @param table The table
     * @return The SQL table name
     */
    private String tableName(Table table) {
        return driver.getTableName(table);
    }


    /**
     * Get the SQL column name from the given column
     *
     * @param column The column model
     * @return The SQL column name
     */
    private String columnName(Column column) {
        return driver.getColumnName(column);
    }


    /**
     * Get the SQL index name from the given column
     *
     * @param index The index model
     * @return The SQL index name
     */
    private String indexName(Index index) {
        return driver.getIndexName(index);
    }


    private Connection con() {
        return supplier.get();
    }


}
