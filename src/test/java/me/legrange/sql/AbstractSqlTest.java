package me.legrange.sql;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import me.legrange.sql.driver.MySql;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeAll;

import javax.sql.DataSource;
import java.sql.SQLException;

class AbstractSqlTest {

    private static DataSource jdbcDataSource;
    protected static SqlManager manager;

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
                break;
            case "h2":
            default:
                driver = new MySql();
                jdbcDataSource = setupH2DataSource();
        }
        say("Using %s data source", dbType);
         manager = new SqlManager(() -> {
            try {
                return jdbcDataSource.getConnection();
            } catch (SQLException ex) {
                throw new RuntimeException(ex.getMessage(), ex);
            }
        }, driver);
         manager.setDeleteMissingColumns(true);
    }

    protected static final void say(String fmt, Object... args) {
        System.out.printf(fmt, args);
        System.out.println();
    }

    private static DataSource setupH2DataSource() {
        JdbcDataSource jdbcDataSource = new JdbcDataSource();
        jdbcDataSource.setUrl("jdbc:h2:mem:neutral;DB_CLOSE_DELAY=-1;INIT=CREATE SCHEMA IF NOT EXISTS neutral;MODE=MYSQL;DATABASE_TO_UPPER=false");
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
}


