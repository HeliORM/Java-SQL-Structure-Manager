package me.legrange.sql;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import me.legrange.sql.driver.MySql;
import me.legrange.sql.driver.PostgreSql;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeAll;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class AbstractSqlTest {

    private static DataSource jdbcDataSource;
    protected static SqlModeller modeller;
    protected static SqlVerifier manager;
    protected static TestDatabase db = new TestDatabase("neutral");
    protected static TestTable table = new TestTable(db, "Person");

    @BeforeAll
    public static void setup() throws Exception {
        String dbType = System.getenv("TEST_DB");
        dbType = (dbType == null) ? "" : dbType;
        Driver driver = null;
        switch (dbType) {
            case "mysql":
                jdbcDataSource = setupMysqlDataSource();
                driver = new MySql();
                break;
            case "postgresql":
                jdbcDataSource = setupPostgreSqlDatasource();
                driver = new PostgreSql();
                break;
            case "h2":
            default:
                driver = new MySql();
                jdbcDataSource = setupH2DataSource();
        }
        say("Using %s data source", dbType);
         manager = new SqlVerifier(() -> {
            try {
                return jdbcDataSource.getConnection();
            } catch (SQLException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }, driver);
         manager.setDeleteMissingColumns(true);
         modeller = new SqlModeller(() -> {
             try {
                 return jdbcDataSource.getConnection();
             } catch (SQLException ex) {
                 throw new RuntimeException(ex.getMessage(), ex);
             }
         }, driver);
    }

    protected static final void say(String fmt, Object... args) {
        System.out.printf(fmt, args);
        System.out.println();
    }

    private static DataSource setupH2DataSource() {
        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setUrl("jdbc:h2:~/neutral;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS neutral;MODE=MYSQL;DATABASE_TO_UPPER=false;WRITE_DELAY=0");
        return jdbcDataSource;
    }

    private static DataSource setupMysqlDataSource() throws SQLException {
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/neutral");
        conf.setUsername("root");
        conf.setPassword("dev");
        HikariDataSource ds = new HikariDataSource(conf);
        return ds;
    }


    private static DataSource setupPostgreSqlDatasource() throws SQLException {
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/neutral");
        conf.setUsername("postgres");
        conf.setPassword("dev");
        HikariDataSource ds = new HikariDataSource(conf);
        return ds;
    }

    protected boolean isSameTable(Table one, TestTable other) {
        return one.getDatabase().getName().equals(other.getDatabase().getName())
                && isSameColumns(one.getColumns(), other.getColumns())
               && isSameIndexes(one.getIndexes(), other.getIndexes());
    }

    protected boolean isSameColumns(Set<Column> one, Set<Column> other) {
        if (one.size() != other.size()) {
            return false;
        }
        Map<String, Column> oneMap = one.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Column> otherMap = other.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        for (String name : oneMap.keySet()) {
            if (!otherMap.containsKey(name)) {
                return false;
            }
            if (!isSameColumn(oneMap.get(name), otherMap.get(name))) {
                return false;
            }
        }
        return true;
    }

    protected boolean isSameColumn(Column one, Column other) {
        boolean same = one.isAutoIncrement() == other.isAutoIncrement()
                && one.isNullable() == other.isNullable()
                && one.isKey() == other.isKey()
                && one.getLength().equals(other.getLength())
                && one.getName().equals(other.getName())
                && one.getJavaType().equals(other.getJavaType())
                && one.getJdbcType().equals(other.getJdbcType());
        if (!same) {
            System.out.println("" + one + "\nvs\n" + other);
        }
        return same;
    }

    protected boolean isSameIndexes(Set<Index> one, Set<Index>other) {
        if (one.size() != other.size()) {
            return false;
        }
        Map<String, Index> oneMap = one.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Index> otherMap = other.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        for (String name : oneMap.keySet()) {
            if (!otherMap.containsKey(name)) {
                return false;
            }
            if (!isSameIndex(oneMap.get(name), otherMap.get(name))) {
                return false;
            }
        }
        return true;
    }

    protected boolean isSameIndex(Index one, Index other) {
        boolean same = one.getName().equals(other.getName())
                && (one.isUnique() == other.isUnique());
        if (same) {
            return isSameColumns(one.getColumns(), other.getColumns());
        }
        return false;
    }

}


