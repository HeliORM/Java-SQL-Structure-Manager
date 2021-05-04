package me.legrange.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static me.legrange.sql.Types.findJavaType;

public class SqlManager {

    private final Supplier<Connection> supplier;
    private final Driver driver;
    private final boolean deleteMissingColumns = false;

    SqlManager(Supplier<Connection> supplier, Driver driver) {
        this.supplier = supplier;
        this.driver = driver;
    }

    public List<Action> verifyTable(Table table) throws SqlManagerException {
        if (!tableExists(table)) {
            return Collections.singletonList(createTable(table));
        } else {
            return verifyStructure(table);
        }
    }

    private boolean tableExists(Table table) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            try (ResultSet tables = dbm.getTables(databaseName(table), null, tableName(table), null)) {
                return tables.next();
            }
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error checking table '%s' (%s)", ex.getMessage()));
        }
    }

    private Action createTable(Table table) throws SqlManagerException {
        String sql = makeCreateTableQuery(table);
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            return Action.createTable(table);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error creating table '%s' (%s)", table.getName(), ex.getMessage()));
        }
    }

    private List<Action> verifyStructure(Table table) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            Map<String, Column> tableColumns = table.getColumns().stream()
                    .collect(Collectors.toMap(col -> col.getName(), col -> col));
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
            List<Action> actions = new ArrayList<>();
            for (String name : tableColumns.keySet()) {
                Column tableColumn = tableColumns.get(name);
                if (!sqlColumns.containsKey(name)) {
                    actions.add(addColumn(tableColumn));
                } else {
                    Column sqlColumn = sqlColumns.get(name);
                    if (!tableColumn.equals(sqlColumn)) {
                        actions.add(modifyColumn(sqlColumn, tableColumn));
                    }
                }
            }
            for (String name : sqlColumns.keySet()) {
                Column sqlColumn = sqlColumns.get(name);
                if (!tableColumns.containsKey(name)) {
                    if (deleteMissingColumns) {
                        actions.add(deleteColumn(sqlColumn));
                    }
                }
            }
            return actions;
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error verifying table '%s' (%s)", table.getName(), ex.getMessage()));
        }
    }

    private Action modifyColumn(Column current, Column changed) throws SqlManagerException {
        if (!current.getName().equals(changed.getName())) {
            return renameColumn(current, changed);
        }
        else {
            return changeColumn(current, changed);
        }
    }

    private Action changeColumn(Column current, Column changed) throws SqlManagerException {
        String sql = makeChangeColumnQuery(current, changed);
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            return Action.modifyColumn(changed);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding changing '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }

    private Action renameColumn(Column current, Column changed) throws SqlManagerException {
        String sql = makeRenameColumnQuery(current, changed);
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            return Action.renameColumn(current, changed);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding renaming '%s' in table '%s' (%s)", current.getName(), current.getTable().getName(), ex.getMessage()));
        }
    }

    private Action deleteColumn(Column column) throws SqlManagerException {
        String sql = makeDeleteColumnQuery(column);
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            return Action.deleteColumn(column);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error deleting column '%s' from table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()));
        }
    }


    private Action addColumn(Column column) throws SqlManagerException {
        String sql = makeAddColumnQuery(column);
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(sql);
            return Action.addColumn(column);
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error adding column '%s' to table '%s' (%s)", column.getName(), column.getTable().getName(), ex.getMessage()));
        }
    }

    private String makeRenameColumnQuery(Column current, Column changed) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("ALTER TABLE %s RENAME COLUMN %s TO %s",
                tableName(current.getTable()),
                columnName(current),
                columnName(changed)));
        return sql.toString();
    }


    private String makeDeleteColumnQuery(Column column) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("ALTER TABLE %s DROP COLUMN %s",
                tableName(column.getTable()),
                columnName(column)));
        return sql.toString();
    }

    private String makeChangeColumnQuery(Column current, Column changed) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("ALTER TABLE %s MODIFY COLUMN %s %s",
                tableName(current.getTable()),
                columnName(current),
                driver.getCreateType(changed)));
        return sql.toString();
    }

    private String makeAddColumnQuery(Column column) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("ALTER TABLE %s ADD COLUMN %s %s",
                tableName(column.getTable()),
                columnName(column),
                driver.getCreateType(column)));
        return sql.toString();
    }

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
            boolean nullable  = rs.getString("IS_NULLABLE").equals("YES");
            boolean autoIncrement  = rs.getString("IS_AUTOINCREMENT").equals("YES");
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

    private Connection con() {
        return supplier.get();
    }

    private String databaseName(Table table) {
        return driver.getDatabaseName(table.getDatabase());

    }

    private String tableName(Table table) {
        return driver.getTableName(table);
    }

    private String columnName(Column column) {
        return driver.getColumnName(column);
    }


}
