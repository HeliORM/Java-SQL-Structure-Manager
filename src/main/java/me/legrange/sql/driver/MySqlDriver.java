package me.legrange.sql.driver;

import me.legrange.sql.BitColumn;
import me.legrange.sql.BooleanColumn;
import me.legrange.sql.Column;
import me.legrange.sql.Database;
import me.legrange.sql.DecimalColumn;
import me.legrange.sql.EnumColumn;
import me.legrange.sql.Index;
import me.legrange.sql.SetColumn;
import me.legrange.sql.StringColumn;
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
        } else if (column instanceof StringColumn)  {
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
            typeName = format("DECIMAL(%d,%d)",((DecimalColumn) column).getPrecision(), ((DecimalColumn) column).getScale());
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
    public boolean isEnumColumn(String columnName, JDBCType jdbcType, String typeName) {
        return typeName.equals("ENUM");
    }

    @Override
    public String makeReadEnumQuery(EnumColumn column) {
        return format("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' " +
                "AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", column.getTable().getDatabase().getName(), column.getTable().getName(), column.getName());
    }

    @Override
    public String makeRenameIndexQuery(Index current, Index changed) {
        return format("ALTER TABLE %s RENAME INDEX %s TO %s", getTableName(current.getTable()), getIndexName(current), getIndexName(changed));
    }

    @Override
    public Set<String> extractEnumValues(String text) {
        return Arrays.stream(text.replace("enum", "").replace("(", "").replace(")", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }

    @Override
    public boolean isSetColumn(String columnName, JDBCType jdbcType, String typeName) {
        return typeName.equals("SET");
    }


    @Override
    public String makeReadSetQuery(SetColumn column) {
        return format("SELECT SUBSTRING(COLUMN_TYPE,5) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' " +
                "AND TABLE_NAME='%s' AND COLUMN_NAME='%s'", column.getTable().getDatabase().getName(), column.getTable().getName(), column.getName());
    }

    @Override
    public Set<String> extractSetValues(String text) {
        return Arrays.stream(text.replace("enum", "").replace("(", "").replace(")", "")
                        .split(","))
                .map(val -> val.substring(1, val.length() - 1))
                .collect(Collectors.toSet());
    }


    @Override
    public boolean typesAreCompatible(Column one, Column other) {
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
        }
        else if (one instanceof BooleanColumn) {
            if (other instanceof BooleanColumn) {
                return true;
            }
            else if (other instanceof BitColumn) {
                return ((BitColumn) other).getBits() == 1;
            }
        } else if (one instanceof StringColumn) {
            if (other instanceof StringColumn) {
                return actualTextLength((StringColumn) one) == actualTextLength((StringColumn) other);
            }
        }
        else if (one instanceof DecimalColumn) {
            if (other instanceof DecimalColumn) {
                return ((DecimalColumn) one).getPrecision() == ((DecimalColumn) other).getPrecision()
                        && ((DecimalColumn) one).getScale() == ((DecimalColumn) other).getScale();
            }
        }
        return one.getJdbcType() == other.getJdbcType();
    }

}
