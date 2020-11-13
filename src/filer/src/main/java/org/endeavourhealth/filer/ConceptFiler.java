package org.endeavourhealth.filer;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.filer.models.FilerConstants;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Properties;

public class ConceptFiler {

    private static final Logger LOG = LoggerFactory.getLogger(ConceptFiler.class);
    public static final String CONCEPTS_FILENAME = "concepts.zip";

    public static void main(String[] args) {

        LOG.info("Starting Subscriber Server Concepts updater");

        Properties properties = null;
        try {
            properties = FilerUtil.initialize();
        } catch (Exception e) {
            LOG.error("Error in reading config.properties " + e.getMessage());
            System.exit(-1);
        }

        try {

            File stagingDir = new File(properties.getProperty(FilerConstants.STAGING));
            File successDir = new File(properties.getProperty(FilerConstants.SUCCESS));
            File failureDir = new File(properties.getProperty(FilerConstants.FAILURE));
            FilerUtil.setupDirectories(stagingDir, successDir, failureDir);

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            File conceptsFile = null;

            try {
                sftp.open();
                LOG.info("Downloading file: " + CONCEPTS_FILENAME);
                InputStream inputStream = sftp.getFile(properties.getProperty(FilerConstants.INCOMING), CONCEPTS_FILENAME);
                conceptsFile = new File(stagingDir.getAbsolutePath() + File.separator + CONCEPTS_FILENAME);
                Files.copy(inputStream, conceptsFile.toPath());
                inputStream.close();
                LOG.info("Deleting file: " + CONCEPTS_FILENAME + " from SFTP server.");
                sftp.deleteFile(CONCEPTS_FILENAME);
                sftp.close();
            } catch (Exception e) {
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                System.exit(-1);
            }

            File files[] = new File[] { conceptsFile };
            FilerUtil.decryptFiles(files, properties);
            LOG.info("Deflating zip file: " + conceptsFile.getName());
            ZipFile zipFile = new ZipFile(conceptsFile);
            String destPath = stagingDir.getAbsolutePath() + File.separator + CONCEPTS_FILENAME.substring(0, CONCEPTS_FILENAME.length() - 4);
            File conceptDir = new File(destPath);
            conceptDir.mkdirs();
            zipFile.extractAll(destPath);

            Connection connection = FilerUtil.getConnection(properties);
            connection.setAutoCommit(false);
            LOG.info("Database connection established.");

            files = FilerUtil.getFilesFromDirectory(destPath, ".sql");
            for (File file : files) {
                LOG.info("Running statements form file: " + file.getName());
                try (BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
                    int i = 0;
                    String statement;
                    Statement batch = connection.createStatement(
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    while ((statement = br.readLine()) != null) {
                        try {
                            i++;
                            batch.addBatch(statement);
                            if(i % 5000 == 0 ) {
                                int[] executed = batch.executeBatch();
                                LOG.info("Executed statements:" + executed.length);
                                connection.commit();
                                System.gc();
                            }
                        } catch (SQLException e) {
                            LOG.error("Reason: " + e.getMessage());
                            break;
                        }
                    }
                    int[] executed = batch.executeBatch();
                    LOG.info("Executed statements:" + executed.length);
                    connection.commit();
                } catch (IOException e) {
                }
            }

            files = FilerUtil.getFilesFromDirectory(destPath, ".setup");
            for (File file : files) {
                LOG.info("Running statements form file: " + file.getName());
                try (BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
                    String statement;
                    while ((statement = br.readLine()) != null) {
                        Statement stmt = connection.createStatement();
                        try {
                            LOG.debug(statement);
                            stmt.execute(statement);
                        } catch (SQLException e) {
                        }
                    }
                    connection.commit();
                } catch (IOException e) {
                }
            }

            ArrayList<String> spNames = new ArrayList<>();
            files = FilerUtil.getFilesFromDirectory(destPath, ".execute");
            for (File file : files) {
                LOG.info("Running statements form file: " + file.getName());
                String schema = FilenameUtils.removeExtension(file.getName());
                try (BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
                    String spName;
                    String query;
                    while ((spName = br.readLine()) != null) {
                        if (isSqlServer(connection)) {
                            query = "exec " + schema + ".dbo." + spName + ";";
                        } else {
                            query = "{ call " + schema + "." + spName + "};";
                        }
                        spNames.add(spName);
                        LOG.info("Executing stored proc: " + spName);
                        PreparedStatement ps = connection.prepareStatement(query);
                        ps.execute();
                        LOG.info("Finished stored procedure processing.");
                    }
                } catch (IOException e) {
                }
            }

            for (String spName : spNames) {
                String query = "DROP PROCEDURE IF EXISTS ";
                Statement stmt = connection.createStatement();
                try {
                    LOG.info("Dropping stored proc: " + spName);
                    stmt.execute(query + spName + ";");
                } catch (SQLException e) {
                }
            }
            connection.commit();

            FileUtils.deleteDirectory(conceptDir);
            FileUtils.forceDelete(conceptsFile);

        } catch (Exception e) {
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            System.exit(-1);
        }

        LOG.info("Ending Subscriber Server Concepts uploader");
        System.exit(0);
    }

    public static boolean isSqlServer(Connection connection) {
        if (connection instanceof SQLServerConnection) {
            return true;
        } else {
            try {
                connection.unwrap(SQLServerConnection.class);
                return true;
            } catch (SQLException var2) {
                return false;
            }
        }
    }
}
