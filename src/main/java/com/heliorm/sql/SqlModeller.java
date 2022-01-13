package com.heliorm.sql;

import com.heliorm.sql.mysql.MysqlModeller;
import com.heliorm.sql.postgres.PostgresModeller;

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

/**
 * A modeller allows us to model SQL database structures into Java structures and modify SQL database structures.
 * This class must be extended to provide support for specific database types.
 */
public abstract class SqlModeller {
    private final Supplier<Connection> supplier;

    /**
     * Create a modeller for MySQL/MariaDB databases.
     *
     * @param supplier A supplier of SQL connections.
     * @return The modeller
     */
    public static SqlModeller mysql(Supplier<Connection> supplier) {
        return new MysqlModeller(supplier);
    }

    /**
     * Create a modeller for PostgreSQL databases.
     *
     * @param supplier A supplier of SQL connections.
     * @return The modeller
     */
    public static SqlModeller postgres(Supplier<Connection> supplier) {
        return new PostgresModeller(supplier);
    }

    /**
     * Generate a text SQL schema for a table.
     *
     * @param table The table
     * @return The schema
     * @throws SqlModellerException
     */
    public final String generateSchema(Table table) throws SqlModellerException {
        return makeCreateTableQuery(table);
    }

    /**
     * Read a database from SQL and return a model for it.
     *
     * @param name The name of the database to read
     * @return The model
     * @throws SqlModellerException Thrown if there is a problem reading the model
     */
    public final Database readDatabase(String name) throws SqlModellerException {
        SqlDatabase database = new SqlDatabase(name);
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(null, null, null, null)) {
                while (tables.next()) {
                    database.addTable(readTable(database, tables.getString("TABLE_NAME")));
                }
            }
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error scanning database '%s' (%s)", name, ex.getMessage()), ex);
        }
        return database;
    }

    /**
     * Read a table from SQL and return a model for it.
     *
     * @param database The database for the table
     * @param name     The name of the table
     * @return The table model
     * @throws SqlModellerException Thrown if there is a problem reading the model
     */
    public final Table readTable(Database database, String name) throws SqlModellerException {
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
                        throw new SqlModellerException(format("Cannot find column '%s' in table '%s' yet it is a primary key", keys.getString("COLUMN_NAME"), table.getName()));
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
            throw new SqlModellerException(format("Error scanning table '%s' (%s)", name, ex.getMessage()), ex);
        }
    }

    /**
     * Deterime if a table exists in SQL
     *
     * @param table The table
     * @return Does it exist?
     * @throws SqlModellerException Thrown if there is a problem
     */
    public final boolean tableExists(Table table) throws SqlModellerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(getDatabaseName(table.getDatabase()), null, table.getName(), null)) {
                return tables.next();
            }
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error checking table '%s' (%s)", table.getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Create a table based on a table model.
     *
     * @param table The table model
     * @throws SqlModellerException Thrown if there is a problem creating the table
     */
    public final void createTable(Table table) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeCreateTableQuery(table));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error creating table '%s' (%s)", table.getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Delete a table from SQL
     *
     * @param table The table model
     * @throws SqlModellerException Thrown if there is a problem deleting the table
     */
    public final void deleteTable(Table table) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeDeleteTableQuery(table));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error deleting table '%s' (%s)", table.getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Add a column to a table.
     *
     * @param column The column to add
     * @throws SqlModellerException Thrown if there is a problem adding the column
     */
    public final void addColumn(Column column) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeAddColumnQuery(column));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error adding column '%s' to table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Rename a column.
     *
     * @param current The current column
     * @param changed The changed column
     * @throws SqlModellerException Thrown if there is a problem reaming the column
     */
    public final void renameColumn(Column current, Column changed) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRenameColumnQuery(current, changed));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error renaming column '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Delete a column from SQL
     *
     * @param column The column to delete
     * @throws SqlModellerException Thrown if there is a problem deleting the column
     */
    public final void deleteColumn(Column column) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeDeleteColumnQuery(column));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error deleting column '%s' from table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Modify a column in SQL.
     *
     * @param current The current column
     * @throws SqlModellerException Thrown if there is a problem modifying the model
     */
    public void modifyColumn(Column current) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeModifyColumnQuery(current));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error modifying column '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Add an index to a SQL table.
     *
     * @param index The index to add
     * @throws SqlModellerException
     */
    public final void addIndex(Index index) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeAddIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error adding index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Rename an index on a SQL table.
     *
     * @param current The index to modify
     * @param changed The changed index
     * @throws SqlModellerException
     */
    public final void renameIndex(Index current, Index changed) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRenameIndexQuery(current, changed));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error renaming index '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Modify an index on a SQL table
     *
     * @param index The index to modify
     * @throws SqlModellerException
     */
    public abstract void modifyIndex(Index index) throws SqlModellerException;

    /**
     * Check if a modeller supports SET types
     *
     * @return True if it does
     */
    public abstract boolean supportsSet();

    /**
     * Remove an index from a SQL table.
     *
     * @param index The index to remove
     * @throws SqlModellerException
     */
    public final void removeIndex(Index index) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(makeRemoveIndexQuery(index));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error removing index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    /**
     * Create a new modeller with the given connection supplier and driver.
     *
     * @param supplier The connection supplier
     */
    protected SqlModeller(Supplier<Connection> supplier) {
        this.supplier = supplier;
    }

    /**
     * Compare two columns by their typing. Returns true if they are essentially the same. Must be provided
     * by a database specific implementation.
     *
     * @param one   One column
     * @param other The other column
     * @return True if the same
     */
    protected abstract boolean typesAreCompatible(Column one, Column other);

    /**
     * Extract the allowed values of a Set type.
     *
     * @param string The string from the database.
     * @return The set of strings.
     */
    protected abstract Set<String> extractSetValues(String string);

    /**
     * Determine if a column is a SET column
     *
     * @param colunmName The column name
     * @param jdbcType   The column type
     * @param typeName   The column type name
     * @return True if it is a set.
     */
    protected abstract boolean isSetColumn(String colunmName, JDBCType jdbcType, String typeName);

    /**
     * Read the possible enum values for a ENUM column
     *
     * @param column The column
     * @return The set values.
     * @throws SqlModellerException
     */
    protected abstract Set<String> readEnumValues(EnumColumn column) throws SqlModellerException;

    /**
     * Generate SQL statement to add an index to a table.
     *
     * @param index The index
     * @return The SQL
     */
    protected final String makeAddIndexQuery(Index index) {
        return format("CREATE %sINDEX %s on %s (%s)",
                index.isUnique() ? "UNIQUE " : "",
                getIndexName(index),
                getTableName(index.getTable()),
                index.getColumns().stream()
                        .map(this::getColumnName)
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }

    /**
     * Generate SQL statement to create a table.
     *
     * @param table The table
     * @return The SQL
     */
    protected abstract String makeCreateTableQuery(Table table) throws SqlModellerException;

    /**
     * Generate the database specific column name from a column.
     *
     * @param column The column
     * @return The name
     */
    protected abstract String getColumnName(Column column);

    /**
     * Generate the database specific column type as used when creating a column.
     *
     * @param column The column
     * @return The type text
     * @throws SqlModellerException
     */
    protected abstract String getCreateType(Column column) throws SqlModellerException;

    /**
     * Generate the database specific table name from a table.
     *
     * @param table The table
     * @return The name
     */
    protected abstract String getTableName(Table table);


    /**
     * Generate the database specific database name from a database.
     *
     * @param database The database
     * @return The name
     */
    protected abstract String getDatabaseName(Database database);

    /**
     * Get a database connection.
     *
     * @return The connection
     */
    protected final Connection con() {
        return supplier.get();
    }


    /**
     * Generate SQL statement to modify an index.
     *
     * @param index The index
     * @return The SQL
     */
    protected abstract String makeModifyIndexQuery(Index index);

    /**
     * Determine if a column is an ENUM column
     *
     * @param columnName The column name
     * @param jdbcType   The column type
     * @param typeName   The column type name
     * @return True if it is a set.
     */
    protected abstract boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName) throws SqlModellerException;


    /**
     * Determine the effective character length of a string column.
     *
     * @param column The column
     * @return The effective length
     */
    protected final int actualTextLength(StringColumn column) {
        int length = column.getLength();
        if (length >= 16777215) {
            return 2147483647;
        } else if (length > 65535) {
            return 16777215;
        } else if (length > 255) {
            return 65535;
        }
        return 255;
    }

    /**
     * Generate a query to modify a column in a table.
     *
     * @param column The column
     * @return The SQL statement
     * @throws SqlModellerException
     */
    protected abstract String makeModifyColumnQuery(Column column) throws SqlModellerException;

    /**
     * Generate a query to add a column to a table
     *
     * @param column The column to add
     * @return The query
     */
    protected abstract String makeAddColumnQuery(Column column) throws SqlModellerException;

    /**
     * Generate a query to remove an index.
     *
     * @param index The index to remove
     * @return The query
     */
    protected abstract String makeRemoveIndexQuery(Index index);

    /**
     * Get the query syntax index name for the index.
     *
     * @param index The index
     * @return The name
     */
    protected abstract String getIndexName(Index index);

    /**
     * Make a query to read set values
     *
     * @param column The column to read the values for
     * @return The values as a string.
     */
    protected abstract String makeReadSetQuery(SetColumn column) throws SqlModellerException;

    /**
     * Generate a query to rename an index.
     *
     * @param current The current index
     * @param changed The changed index
     * @return The query
     */
    protected abstract String makeRenameIndexQuery(Index current, Index changed);

    /**
     * Read the possible set values for a SET column
     *
     * @param column The column
     * @return The set values.
     * @throws SqlModellerException
     */
    private Set<String> readSetValues(SetColumn column) throws SqlModellerException {
        String query = makeReadSetQuery(column);
        try (Connection con = con(); Statement stmt = con.createStatement(); ResultSet ers = stmt.executeQuery(query)) {
            if (ers.next()) {
                return extractSetValues(ers.getString(1));
            }
            return Collections.EMPTY_SET;
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error reading set values from %s.%s.%s (%s)",
                    column.getTable().getDatabase().getName(), column.getTable().getName(), column.getName(), ex.getMessage()), ex);
        }
    }

    /**
     * Generate a query to delete a table.
     *
     * @param table The table to delete
     * @return The query
     */
    private String makeDeleteTableQuery(Table table) {
        return format("DROP TABLE %s", getTableName(table));
    }

    /**
     * Generate a SQL statement to rename a column in a table.
     *
     * @param column The current column
     * @param column The changed column
     * @return The SQL
     */
    private String makeRenameColumnQuery(Column column, Column changed) {
        return format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getColumnName(changed));
    }

    /**
     * Generate a SQL statement to delete a column from a table.
     *
     * @param column The column to delete
     * @return The SQL
     */
    private String makeDeleteColumnQuery(Column column) {
        return format("ALTER TABLE %s DROP COLUMN %s",
                getTableName(column.getTable()),
                getColumnName(column));
    }

    /**
     * Determine if the given JDBC type represents a string column
     *
     * @param jdbcType The JDBC type
     * @return True if it is
     */
    private boolean isStringColumn(JDBCType jdbcType) {
        switch (jdbcType) {
            case CHAR:
            case VARCHAR:
            case LONGVARCHAR:
                return true;
        }
        return false;
    }

    /**
     * Determine if the given JDBC type represents a date/time column
     *
     * @param jdbcType The JDBC type
     * @return True if it is
     */
    private boolean isDateTimeColumn(JDBCType jdbcType) {
        switch (jdbcType) {
            case DATE:
            case TIME:
            case TIMESTAMP:
                return true;
        }
        return false;
    }

    /**
     * Determine if the given JDBC type represents a binary column
     *
     * @param jdbcType The JDBC type
     * @return True if it is
     */
    private boolean isBinaryColumn(JDBCType jdbcType) {
        switch (jdbcType) {
            case LONGVARBINARY:
            case VARBINARY:
            case BINARY:
            case BLOB:
                return true;
        }
        return false;
    }

    /**
     * Read a column model from a SQL result set.
     *
     * @param table The table for the column
     * @param rs    The result set
     * @return The column mode
     * @throws SqlModellerException
     */
    private SqlColumn getColumnFromResultSet(Table table, ResultSet rs) throws SqlModellerException {
        try {
            JDBCType jdbcType = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
            Optional<Integer> size = Optional.ofNullable(rs.getInt("COLUMN_SIZE"));
            boolean nullable = rs.getString("IS_NULLABLE").equals("YES");
            boolean autoIncrement = rs.getString("IS_AUTOINCREMENT").equals("YES");
            String columnName = rs.getString("COLUMN_NAME");
            String typeName = rs.getString("TYPE_NAME");

            if (isEnumColumn(columnName, jdbcType, typeName)) {
                return new SqlEnumColumn(table, columnName, nullable, readEnumValues(new SqlEnumColumn(table, columnName, nullable, Collections.emptySet())));
            } else if (isSetColumn(columnName, jdbcType, typeName)) {
                return new SqlSetColumn(table, columnName, nullable, readSetValues(new SqlSetColumn(table, columnName, nullable, Collections.emptySet())));
            } else if (isStringColumn(jdbcType)) {
                return new SqlStringColumn(table, columnName, jdbcType, nullable, size.get());
            } else if (isBinaryColumn(jdbcType)) {
                return new SqlBinaryColumn(table, columnName, jdbcType, nullable, size.get());
            }
            if (isDateTimeColumn(jdbcType)) {
                return new SqlDateTimeColumn(table, columnName, jdbcType, nullable);
            }
            switch (jdbcType) {
                case BIT:
                    return new SqlBitColumn(table, columnName, nullable, size.get());
                case BOOLEAN:
                    return new SqlBooleanColumn(table, columnName, nullable);
                case DECIMAL:
                case DOUBLE:
                    return new SqlDecimalColumn(table, columnName, jdbcType, nullable, size.get(), rs.getInt("DECIMAL_DIGITS"));
                case NUMERIC:
                    if (size.isPresent()) {
                        return new SqlDecimalColumn(table, columnName, jdbcType, nullable, size.get(), rs.getInt("DECIMAL_DIGITS"));
                    }
                    return new SqlDecimalColumn(table, columnName, jdbcType, nullable);
                case INTEGER:
                case SMALLINT:
                case BIGINT:
                    return new SqlIntegerColumn(table, columnName, jdbcType, nullable, autoIncrement);
            }
            throw new SqlModellerException(format("Unsupported JDBC type %s in result set. BUG!", jdbcType.getName()));
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error reading SQL column information (%s)", ex.getMessage()), ex);
        }
    }

}
