package com.codechronicle.aws.glacier.dbutil;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;

/**
 * Created with IntelliJ IDEA.
 * User: sroy
 * Date: 9/22/12
 * Time: 3:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class HSQLDBUtil {

    private static Logger log = LoggerFactory.getLogger(HSQLDBUtil.class);

    public static ComboPooledDataSource initializeDatabase(File workingDirectory) {
        if (workingDirectory == null) {
            workingDirectory = new File(System.getenv("HOME") + "/.glacier/hsqldb");
            log.info("Working database directory = " + workingDirectory.getAbsolutePath());

            if (!workingDirectory.exists()) {
                workingDirectory.mkdirs();
            }
        }

        File dbFile = new File(workingDirectory, "glacierdb");
        File dbPropsFile = new File(dbFile.getAbsolutePath() + ".script");
        boolean isFreshDB = !dbPropsFile.exists();

        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        try {
            dataSource.setDriverClass("org.hsqldb.jdbc.JDBCDriver"); //loads the jdbc driver
            dataSource.setJdbcUrl("jdbc:hsqldb:file:" + dbFile.getAbsolutePath());
            dataSource.setUser("SA");
            dataSource.setPassword("");
            dataSource.setAutoCommitOnClose(true);
        } catch (PropertyVetoException e) {
            throw new RuntimeException("Unable to initialize database at " + workingDirectory.getAbsolutePath(), e);
        }

        // Only happens the first time a given working directory is used.
        if (isFreshDB) {
            try {
                initializeSchema(dataSource);

                // Required in order to force the schema changes to disk.
                shutdownDatabase(dataSource);

                // After shutdown, reinitialize database so it's ready for use
                return initializeDatabase(workingDirectory);
            } catch (SQLException e) {
                throw new RuntimeException("Unable to initialize database", e);
            }
        }

        return dataSource;
    }

    public static void shutdownDatabase(DataSource dataSource) {
        log.info("Shutting down database to force schema changes to be persisted");

        String sql = "SHUTDOWN";
        QueryRunner qr = new QueryRunner(dataSource);
        try {
            qr.update(sql);
        } catch (SQLException e) {
            log.error("Unexpected exception", e);
        }
    }


    private static void cleanSchema(DataSource dataSource) throws SQLException {
        log.info("Cleaning database schema");

        executeSQLScript(dataSource, "drop.sql");
    }

    private static void initializeSchema(DataSource dataSource) throws SQLException {
        log.info("Initializing database schema");

        executeSQLScript(dataSource, "schema.sql");
    }

    private static void executeSQLScript(DataSource dataSource, String resourceFileName) throws SQLException {
        String queries[] = readSQL(resourceFileName);
        QueryRunner runner = new QueryRunner(dataSource);
        for (String query : queries) {
            int result = runner.update(query);
            log.info(result + " ==> [" + query + "]");
        }
    }

    private static String[] readSQL(String resourceFileName) {
        InputStream is = null;

        try {
            is = ClassLoader.getSystemResourceAsStream(resourceFileName);
            String sql = IOUtils.toString(is);
            return sql.split(";");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(is);
        }
    }


}
