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

            String schema = null;
            String dateFromFile = null;

            files = FilerUtil.getFilesFromDirectory(destPath, ".schema");
            if (files.length > 0) {
                schema = FilenameUtils.removeExtension(files[0].getName());
            }

            files = FilerUtil.getFilesFromDirectory(destPath, ".date");
            if (files.length > 0) {
                dateFromFile = FilenameUtils.removeExtension(files[0].getName());
            }

            if (schema != null && dateFromFile != null) {
                LOG.info("Calling stored procedure " + schema + ".update_tables_with_core_concept_id using date:" + dateFromFile);
                Date date = new java.sql.Date((new SimpleDateFormat("yyyy-MM-dd").parse(dateFromFile).getTime()));
                String query = "";
                if (isSqlServer(connection)) {
                    query = "exec " + schema + ".dbo.update_tables_with_core_concept_id ?;";
                } else {
                    query = "{ call " + schema + ".update_tables_with_core_concept_id(?) };";
                }
                PreparedStatement ps = connection.prepareStatement(query);
                ps.setDate(1,date);
                ps.execute();
                LOG.info("Finished stored procedure processing.");
            }
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
