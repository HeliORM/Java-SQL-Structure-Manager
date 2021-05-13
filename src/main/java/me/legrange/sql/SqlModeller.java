package me.legrange.sql;

import java.sql.*;
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
            try (ResultSet columns = dbm.getColumns(null, null, tableName(table), null)) {
                while (columns.next()) {
                    SqlColumn column = getColumnFromResultSet(table, columns);
                    sqlColumns.put(column.getName(), column);
                }
            }
            try (ResultSet keys = dbm.getPrimaryKeys(null, null, tableName(table))) {
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
            return table;
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error scanning table '%s' (%s)", name, ex.getMessage()));
        }
    }

    /** Deterime if a table exists in SQL
     *
     * @param table The table
     * @return Does it exist?
     * @throws SqlManagerException Thrown if there is a problem
     */
    public boolean tableExists(Table table) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(databaseName(table), null, tableName(table), null)) {
                return tables.next();
            }
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error checking table '%s' (%s)", table.getName(), ex.getMessage()));
        }
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

    /** Rename a column.
     *
     * @param current
     * @param changed
     * @throws SqlManagerException Thrown if there is a problem reaming the column
     */
    public void renameColumn(Column current, Column changed) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRenameColumnQuery(current, changed));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding renaming '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }


    /** Delete a column from SQL
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

    /** Modify a column in SQL.
     *
     * @param current The current column
     * @throws SqlManagerException Thrown if there is a problem modifying the model
     */
    public void modifyColumn(Column current) throws SqlManagerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeModifyColumnQuery(current));
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding changing '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Make a SQL query to rename a column in a table
     *
     * @param column The column model
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
     * Make a SQL query to delete a table.
     *
     * @param table The table
     * @return The SQL query
     */
    private String makeDeleteTableQuery(Table table) {
        return format("DROP TABLE %s", tableName(table));
    }

    /** Get the SQL database name for a table
     *
     * @param table The table
     * @return The database name
     */
    private String databaseName(Table table) {
        return driver.getDatabaseName(table.getDatabase());

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


    private Connection con() {
        return supplier.get();
    }


}
