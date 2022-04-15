package com.heliorm.sql;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;

class AbstractSqlTest {

    private static DataSource jdbcDataSource;
    protected static SqlModeller modeller;
    protected static SqlVerifier verifier;
    protected static TestDatabase db = new TestDatabase("neutral");
    protected static TestTable table = new TestTable(db, "Person");

    @Container
    public static GenericContainer mariadb = new GenericContainer(DockerImageName.parse("mariadb"))
            .withExposedPorts(3306)
            .withEnv("MYSQL_DATABASE", "neutral")
            .withEnv("MYSQL_ROOT_PASSWORD", "dev");

    @Container
    public static GenericContainer postgres = new GenericContainer(DockerImageName.parse("postgres"))
            .withExposedPorts(5432)
            .withEnv("POSTGRES_DB", "neutral")
            .withEnv("POSTGRES_PASSWORD", "dev");


    @BeforeAll
    public static void setup() throws Exception {
        String dbType = System.getenv("TEST_DB");
        dbType = (dbType == null) ? "" : dbType;
        switch (dbType) {
            case "postgresql":
                jdbcDataSource = setupPostgreSqlDatasource();
                modeller = SqlModeller.postgres(() -> {
                    try {
                        return jdbcDataSource.getConnection();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                });
                break;
            case "mysql":
            default:
                jdbcDataSource = setupMysqlDataSource();
                modeller = SqlModeller.mysql(() -> {
                    try {
                        return jdbcDataSource.getConnection();
                    } catch (SQLException ex) {
                        throw new RuntimeException(ex.getMessage(), ex);
                    }
                });
                break;

        }
        say("Using %s data source", dbType);
        verifier = SqlVerifier.forModeller(modeller);
        verifier.setDeleteMissingColumns(true);
    }

    protected static void say(String fmt, Object... args) {
        System.out.printf(fmt, args);
        System.out.println();
    }

    private static DataSource setupMysqlDataSource() throws SQLException {
        mariadb.start();
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl(format("jdbc:mysql://%s:%d/neutral", mariadb.getHost(), mariadb.getFirstMappedPort()));
        conf.setUsername("root");
        conf.setPassword("dev");
        return new HikariDataSource(conf);
    }


    private static DataSource setupPostgreSqlDatasource() throws SQLException {
        postgres.start();
        HikariConfig conf = new HikariConfig();
        conf.setJdbcUrl(format("jdbc:postgresql://%s:%d/neutral", postgres.getHost(), postgres.getFirstMappedPort()));
        conf.setUsername("postgres");
        conf.setPassword("dev");
        return new HikariDataSource(conf);
    }

    protected boolean isSameTable(Table one, TestTable other) {
        return one.getDatabase().getName().equals(other.getDatabase().getName())
                && isSameColumns(one.getColumns(), other.getColumns())
                && isSameIndexes(one.getIndexes(), other.getIndexes());
    }

    protected boolean isSameColumns(Set<Column> one, Set<Column> other) {
        if (one.size() != other.size()) {
            say("Columns: one.size %d != other.size %d", one.size(), other.size());
            return false;
        }
        Map<String, Column> oneMap = one.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        Map<String, Column> otherMap = other.stream().collect(Collectors.toMap(col -> col.getName(), col -> col));
        for (String name : oneMap.keySet()) {
            if (!otherMap.containsKey(name)) {
                say("other doesn't have %s", name);
                return false;
            }
            if (!isSameColumn(oneMap.get(name), otherMap.get(name))) {
                return false;
            }
        }
        return true;
    }

    protected boolean isSameIndexes(Set<Index> one, Set<Index> other) {
        if (one.size() != other.size()) {
            say("Indexes: one.size %d != other.size %d", one.size(), other.size());
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

    protected boolean isSameColumn(Column one, Column other) {
        boolean same = one.isAutoIncrement() == other.isAutoIncrement()
                && one.isNullable() == other.isNullable()
                && one.isKey() == other.isKey()
                && one.getName().equals(other.getName())
                && ((one.getDefault() != null && other.getDefault() != null && one.getDefault().equals(other.getDefault()))
                || (one.getDefault() == null && other.getDefault() == null))
                && modeller.typesAreCompatible(one, other);
        if (!same) {
            say("one %s\n\tvs\nother %s", one, other);

        }
        return same;
    }

    protected String makeUnique(int length) {
        return UUID.randomUUID().toString().substring(0,length);
    }
}


