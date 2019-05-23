package org.endeavourhealth.uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class Main {

    private static final String JDBC_CLASS_ENV_VAR = "CONFIG_JDBC_CLASS";
    private static final String JDBC_URL_ENV_VAR = "CONFIG_JDBC_URL";
    private static final String JDBC_USER_ENV_VAR = "CONFIG_JDBC_USERNAME";
    private static final String JDBC_PASSWORD_ENV_VAR = "CONFIG_JDBC_PASSWORD";
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        LOG.info("Starting MS SQL Server uploader");

        try {
            Connection con = Main.getMSSqlServerConnection();
            LOG.info("Connection established.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("Ending MS SQL Server uploader");
    }

    private static Connection getMSSqlServerConnection() throws SQLException, ClassNotFoundException {
        Map<String, String> envVars = System.getenv();
        Class.forName(envVars.getOrDefault(JDBC_CLASS_ENV_VAR, "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
        Properties props = new Properties();
        props.setProperty("user", envVars.getOrDefault(JDBC_USER_ENV_VAR,""));
        props.setProperty("password", envVars.getOrDefault(JDBC_PASSWORD_ENV_VAR, ""));
        return DriverManager.getConnection(envVars.getOrDefault(JDBC_URL_ENV_VAR,""), props);
    }

    private static File[] getFilesFromDirectory(String directory, String prefix) {
        final String str = prefix;
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().startsWith(str);
            }
        };
        return new File(directory).listFiles(fileFilter);
    }
}
