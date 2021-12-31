package com.heliorm.sql.mysql;

import com.heliorm.sql.BitColumn;
import com.heliorm.sql.BooleanColumn;
import com.heliorm.sql.Database;
import com.heliorm.sql.DecimalColumn;
import com.heliorm.sql.EnumColumn;
import com.heliorm.sql.SqlModeller;
import com.heliorm.sql.SqlModellerException;
import com.heliorm.sql.Column;
import com.heliorm.sql.Index;
import com.heliorm.sql.SetColumn;
import com.heliorm.sql.StringColumn;
import com.heliorm.sql.Table;

import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.format;

/** An implementation of the SQL modeller that deals with MySQL/MariaDB syntax.
 *
 */
public final class MysqlModeller extends SqlModeller {
    /**
     * Create a new modeller with the given connection supplier.
     *
     * @param supplier The connection supplier
     */
    public MysqlModeller(Supplier<Connection> supplier) {
        super(supplier);
    }

    @Override
    protected String makeCreateTableQuery(Table table) throws SqlModellerException {
        StringJoiner body = new StringJoiner(",");
        for (Column column : table.getColumns()) {
            body.add(format("%s %s", getColumnName(column), getCreateType(column)));
        }
        for (Index index : table.getIndexes()) {
            body.add(format("%sKEY %s (%s)",
                    index.isUnique() ? "UNIQUE " : "",
                    getIndexName(index),
                    index.getColumns().stream()
                            .map(this::getColumnName)
                            .reduce((c1, c2) -> c1 + "," + c2).get()));
        }
        StringBuilder sql = new StringBuilder();
        sql.append(format("CREATE TABLE %s (", getTableName(table)));
        sql.append(body);
        sql.append(")");
        return sql.toString();
    }

    @Override
    public void modifyIndex(Index index) throws SqlModellerException {
        try (Connection con = con(); Statement stmt = con.createStatement()) {
            removeIndex(index);
            addIndex(index);
        } catch (SQLException ex) {
            throw new SqlModellerException(format("Error modifying index '%s' in table '%s' (%s)", index.getName(), index.getTable().getName(), ex.getMessage()));
        }
    }

    @Override
    public boolean supportsSet() {
        return true;
    }

    @Override
    protected boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName) {
        return typeName.equals("ENUM");
    }

    @Override
    protected String getDatabaseName(Database database) {
        return format("`%s`", database.getName());
    }

    @Override
    protected String getTableName(Table table) {
        return format("`%s`", table.getName());
    }

    @Override
    protected String getCreateType(Column column) {
        String typeName = column.getJdbcType().getName();
        StringBuilder type = new StringBuilder();
        if (column instanceof EnumColumn) {
            EnumColumn ec = (EnumColumn) column;
            Set<String> enumValues = ec.getEnumValues();
            typeName = "ENUM("
                    + enumValues.stream()
                    .map(val -> "'" + val + "'")
                    .reduce((v1, v2) -> v1 + "," + v2).get()
                    + ")";
        } else if (column instanceof SetColumn) {
            SetColumn ec = (SetColumn) column;
            Set<String> values = ec.getSetValues();
            typeName = "SET("
                    + values.stream()
                    .map(val -> "'" + val + "'")
                    .reduce((v1, v2) -> v1 + "," + v2).get()
                    + ")";
        } else if (column instanceof StringColumn) {
            int length = ((StringColumn) column).getLength();
            if (length >= 16777215) {
                typeName = "LONGTEXT";
            } else if (length > 65535) {
                typeName = "MEDIUMTEXT";
            } else if (length > 255) {
                typeName = "TEXT";
            } else {
                typeName = format("VARCHAR(%d)", length);
            }
        } else if (column instanceof DecimalColumn) {
            typeName = format("DECIMAL(%d,%d)", ((DecimalColumn) column).getPrecision(), ((DecimalColumn) column).getScale());
        }
        type.append(typeName);
        if (!column.isNullable()) {
            type.append(" NOT NULL");
        }
        if (column.isAutoIncrement()) {
            type.append(" AUTO_INCREMENT");
        }
        if (column.isKey()) {
            type.append(" PRIMARY KEY");
        }
        return type.toString();
    }

    @Override
    protected String getColumnName(Column column) {
        return format("`%s`", column.getName());
    }

    @Override
    protected String getIndexName(Index index) {
        return format("`%s`", index.getName());
    }

    @Override
    protected String makeReadEnumQuery(EnumColumn column) {
        return format("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' " +
                        "AND TABLE_NAME='%s' AND COLUMN_NAME='%s'",
                column.getTable().getDatabase().getName(),
                column.getTable().getName(),
                column.getName());
    }

    @Override
    protected String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER TABLE %s RENAME INDEX %s TO %s", getTableName(current.getTable()), getIndexName(current), getIndexName(changed));
    }

    @Override
    protected final Set<String> extractEnumValues(String text) {
        return Arrays.stream(text.replace("enum", "").replace("(", "").replace(")", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    @Override
    protected boolean isSetColumn(String columnName, JDBCType jdbcType, String typeName) {
        return typeName.equals("SET");
    }

    @Override
    protected String makeReadSetQuery(SetColumn column) {
        return format("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' " +
                "AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", column.getTable().getDatabase().getName(), column.getTable().getName(), column.getName());
    }

    @Override
    protected Set<String> extractSetValues(String text) {
        return Arrays.stream(text.replace("enum", "").replace("(", "").replace(")", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    protected String makeModifyColumnQuery(Column column) {
        return format("ALTER TABLE %s MODIFY COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column));
    }

    @Override
    protected String makeAddColumnQuery(Column column) {
        return format("ALTER TABLE %s ADD COLUMN %s %s",
                getTableName(column.getTable()),
                getColumnName(column),
                getCreateType(column));
    }

    @Override
    protected String makeRemoveIndexQuery(Index index) {
        return format("DROP INDEX %s on %s",
                getIndexName(index),
                getTableName(index.getTable()));
    }

    @Override
    protected String makeModifyIndexQuery(Index index) {
        return format("ALTER %sINDEX %s ON %s %s",
                index.isUnique() ? "UNIQUE " : "",
                getIndexName(index),
                getTableName(index.getTable()),
                index.getColumns().stream()
                        .map(this::getColumnName)
                        .reduce((c1, c2) -> c1 + "," + c2).get());
    }

    @Override
    protected boolean typesAreCompatible(Column one, Column other) {
        if (one instanceof EnumColumn) {
            if (other instanceof EnumColumn) {
                Set<String> ones = ((EnumColumn) one).getEnumValues();
                Set<String> others = ((EnumColumn) other).getEnumValues();
                return ones.stream().allMatch(v -> others.contains(v))
                        && (others.stream().allMatch(v -> ones.contains(v)));
            }
            return false;
        } else if (one instanceof SetColumn) {
            if (other instanceof SetColumn) {
                Set<String> ones = ((SetColumn) one).getSetValues();
                Set<String> others = ((SetColumn) other).getSetValues();
                return ones.stream().allMatch(v -> others.contains(v))
                        && (others.stream().allMatch(v -> ones.contains(v)));
            }
            return false;
        } else if (one instanceof BitColumn) {
            if (other instanceof BitColumn) {
                return ((BitColumn) one).getBits() == ((BitColumn) other).getBits();
            }
            if (other instanceof BooleanColumn) {
                return ((BitColumn) one).getBits() == 1;
            }
            return false;
        } else if (one instanceof BooleanColumn) {
            if (other instanceof BooleanColumn) {
                return true;
            } else if (other instanceof BitColumn) {
                return ((BitColumn) other).getBits() == 1;
            }
            return false;
        } else if (one instanceof StringColumn) {
            if (other instanceof StringColumn) {
                return actualTextLength((StringColumn) one) == actualTextLength((StringColumn) other);
            }
            return false;
        } else if (one instanceof DecimalColumn) {
            if (other instanceof DecimalColumn) {
                return ((DecimalColumn) one).getPrecision() == ((DecimalColumn) other).getPrecision()
                        && ((DecimalColumn) one).getScale() == ((DecimalColumn) other).getScale();
            }
            return false;
        }
        return one.getJdbcType() == other.getJdbcType();
    }

}
