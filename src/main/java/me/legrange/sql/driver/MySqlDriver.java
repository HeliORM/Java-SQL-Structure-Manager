package me.legrange.sql.driver;

import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.Index;
import me.legrange.sql.Table;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class MySqlDriver extends GenericSqlDriver {

    public MySqlDriver() {
    }

    @Override
    public String getDatabaseName(Database database) {
        return format("`%s`", database.getName());
    }

    @Override
    public String getTableName(Table table) {
        return format("`%s`", table.getName());
    }

    @Override
    public String getCreateType(Column column) {
        StringBuilder type = new StringBuilder();
        String typeName = column.getJdbcType().getName();
        boolean useLength = false;
        switch (column.getJdbcType()) {
            case LONGVARCHAR:
            case VARCHAR:
                if (column.getLength().isPresent()) {
                    int length = column.getLength().get();
                    if (length >= 16777215) {
                        typeName = "LONGTEXT";
                    } else if (length > 65535) {
                        typeName = "MEDIUMTEXT";
                    } else if (length > 255) {
                        typeName = "TEXT";
                    } else {
                        typeName = "VARCHAR";
                        useLength = true;
                    }
                }
                break;
            case CHAR: {
                if (column.getEnumValues().isPresent()) {
                    Set<String> enumValues = column.getEnumValues().get();
                    useLength = false;
                    typeName = "ENUM("
                            + enumValues.stream()
                            .map(val -> "'" + val + "'")
                            .reduce((v1, v2) -> v1 + "," + v2).get()
                            + ")";
                }
            }
            break;
        }
        type.append(typeName);
        if (useLength) {
            type.append(format("(%d)", column.getLength().get()));
        }
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
    public String getColumnName(Column column) {
        return format("`%s`", column.getName());
    }

    @Override
    public String getIndexName(Index index) {
        return format("`%s`", index.getName());
    }

    @Override
    public boolean supportsAlterIndex() {
        return false;
    }

    @Override
    public String makeReadEnumQuery(Column column) {
        return format("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' " +
                "AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", column.getTable().getDatabase().getName(), column.getTable().getName(), column.getName());
    }

    @Override
    public Set<String> extractEnumValues(String text) {
        return Arrays.stream(text.replace("enum", "").replace("(", "").replace(")", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    @Override
    public boolean typesAreCompatible(Column one, Column other) {
        switch (one.getJdbcType()) {
            case BIT:
                switch (other.getJdbcType()) {
                    case BIT:
                        return true;
                    case BOOLEAN:
                        return (!one.getLength().isPresent() || one.getLength().get() == 1);
                    default:
                        return false;
                }
            case BOOLEAN:
                switch (other.getJdbcType()) {
                    case BOOLEAN:
                        return true;
                    case BIT:
                        return (!other.getLength().isPresent() || other.getLength().get() == 1);
                    default:
                        return false;
                }
            case VARCHAR:
            case LONGVARCHAR:
                switch (other.getJdbcType()) {
                    case VARCHAR:
                    case LONGVARCHAR: {
                        return actualTextLength(one) == actualTextLength(other);
                    }
                    default:
                        return false;
                }
            case CHAR:
                if (one.getEnumValues().isPresent()) {
                    if (other.getEnumValues().isPresent()) {
                        Set<String> ones = one.getEnumValues().get();
                        Set<String> others = other.getEnumValues().get();
                        return  ones.stream().allMatch(v -> others.contains(v))
                                && (others.stream().allMatch(v -> ones.contains(v)));

                    }
                    return false;
                }
                else {
                    return !other.getEnumValues().isPresent();
                }
            default:
                return one.getJdbcType() == other.getJdbcType();
        }
    }

}
