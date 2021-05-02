package me.legrange.sql;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static me.legrange.sql.Types.findJavaType;

public class SqlManager {

    private final Supplier<Connection> supplier;
    private final Driver driver;
    private final boolean deleteMissingColumns = false;

    private SqlManager(Supplier<Connection> supplier, Driver driver) {
        this.supplier = supplier;
        this.driver = driver;
    }

    public List<Action> verifyTable(Table table) throws SqlManagerException {
        if (!tableExists(table)) {
            Collections.singletonList(createTable(table));
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
            throw new SqlManagerException(format("Error creating table '%s' (%s)", ex.getMessage()));
        }
    }

    private List<Action> verifyStructure(Table table) throws SqlManagerException {
        try (Connection con = con()) {
            DatabaseMetaData dbm = con.getMetaData();
            Map<String, Column> tableColumns = table.getColumns().stream()
                    .collect(Collectors.toMap(col -> col.getName(),col -> col));
            Map<String, Column> sqlColumns = new HashMap<>();
            try (ResultSet columns = dbm.getColumns(null, null,tableName(table), null)) {
                while (columns.next()) {
                    Column column = getColumnFromResultSet(columns);
                    sqlColumns.put(column.getName(), column);
                }
            }
            List<Action> actions = new ArrayList<>();
            for (String name : tableColumns.keySet()) {
                Column tableColumn = tableColumns.get(name);
                if (!sqlColumns.containsKey(name)) {
                    actions.add(addColumn(tableColum));
                }
                else {
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
        } catch (SQLException ex) {
            throw new SqlManagerException(format("Error verifying table '%s' (%s)", ex.getMessage()));
        }
    }

    private String makeCreateTableQuery(Table table) {
        StringBuilder sql = new StringBuilder();
        sql.append(format("CREATE TABLE %s (", tableName(table)));
        for (Column column  : table.getColumns()) {
            sql.append()
        }
        sql.append(")");
    }

    private Column getColumnFromResultSet(ResultSet rs) throws SqlManagerException {
        try {
            JDBCType jdbcType = JDBCType.valueOf(rs.getInt("DATA_TYPE"));
            return new SqlColumn(
                    rs.getString("COLUMN_NAME"),
                    jdbcType,
                    findJavaType(jdbcType));
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



}
