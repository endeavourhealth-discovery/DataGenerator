package org.endeavourhealth.filer;

import net.lingala.zip4j.core.ZipFile;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MainAdhoc {

    private static final Logger LOG = LoggerFactory.getLogger(MainAdhoc.class);
    public static final String ADHOC_FILENAME = "adhoc.zip";

    public static void main(String[] args) {

        LOG.info("Starting MS SQL Server Adhoc updater");

        FilerUtil.initialize(args);

        try {

            File stagingDir = new File(args[0]);
            FilerUtil.setupStagingDir(stagingDir);

            SftpUtil sftp = FilerUtil.setupSftp(args);
            File adhocFile = null;

            try {
                sftp.open();
                LOG.info("Downloading file: " + ADHOC_FILENAME);
                InputStream inputStream = sftp.getFile(args[4], ADHOC_FILENAME);
                adhocFile = new File(stagingDir.getAbsolutePath() + File.separator + ADHOC_FILENAME);
                Files.copy(inputStream, adhocFile.toPath());
                inputStream.close();
                LOG.info("Deleting file: " + ADHOC_FILENAME + " from SFTP server.");
                sftp.deleteFile(ADHOC_FILENAME);
                sftp.close();
            } catch (Exception e) {
                LOG.info("");
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            File files[] = new File[] { adhocFile };
            FilerUtil.decryptFiles(files, args);
            LOG.info("Deflating zip file: " + adhocFile.getName());
            ZipFile zipFile = new ZipFile(adhocFile);
            String destPath = stagingDir.getAbsolutePath() + File.separator + ADHOC_FILENAME.substring(0, ADHOC_FILENAME.length() - 4);
            File dir = new File(destPath);
            dir.mkdirs();
            zipFile.extractAll(destPath);

            Connection connection = FilerUtil.getMSSqlServerConnection();
            connection.setAutoCommit(false);
            LOG.info("Database connection established.");

            files = FilerUtil.getFilesFromDirectory(destPath, ".sql");
            for (File file : files) {
                LOG.info("Running statements form file: " + file.getName());
                try (BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
                    String statement;
                    while ((statement = br.readLine()) != null) {
                        PreparedStatement preparedStatement = connection.prepareStatement(statement);
                        try {
                            int[] results = preparedStatement.executeBatch();
                            LOG.info("Successfully executed: " + statement);
                            LOG.info("Affected records: " + results.length);
                            connection.commit();
                            preparedStatement.close();
                        } catch (SQLException e) {
                            LOG.info("Failed in executing: " + statement);
                            connection.rollback();
                        }
                    }
                } catch (IOException e) {
                }
            }


        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        LOG.info("Ending MS SQL Server uploader");
    }
}
