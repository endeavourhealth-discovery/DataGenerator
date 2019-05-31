package org.endeavourhealth.filer;

import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class MainAdhoc {

    private static final Logger LOG = LoggerFactory.getLogger(MainAdhoc.class);
    public static final String ADHOC_FILENAME = "adhoc.zip";

    public static void main(String[] args) {

        LOG.info("Starting MS SQL Server Adhoc updater");

        Properties properties = null;
        try {
            properties = FilerUtil.initialize();
        } catch (Exception e) {
            LOG.info("");
            LOG.error("Error in reading config.properties " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        try {

            File stagingDir = new File(properties.getProperty(FilerUtil.STAGING));
            FilerUtil.setupStagingDir(stagingDir);

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            File adhocFile = null;

            try {
                sftp.open();
                LOG.info("Downloading file: " + ADHOC_FILENAME);
                InputStream inputStream = sftp.getFile(properties.getProperty(FilerUtil.LOCATION), ADHOC_FILENAME);
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
            FilerUtil.decryptFiles(files, properties);
            LOG.info("Deflating zip file: " + adhocFile.getName());
            ZipFile zipFile = new ZipFile(adhocFile);
            String destPath = stagingDir.getAbsolutePath() + File.separator + ADHOC_FILENAME.substring(0, ADHOC_FILENAME.length() - 4);
            File dir = new File(destPath);
            dir.mkdirs();
            zipFile.extractAll(destPath);

            Connection connection = FilerUtil.getMSSqlServerConnection(properties);
            connection.setAutoCommit(false);
            LOG.info("Database connection established.");

            boolean success = true;
            files = FilerUtil.getFilesFromDirectory(destPath, ".sql");
            for (File file : files) {
                LOG.info("Running statements form file: " + file.getName());
                try (BufferedReader br = Files.newBufferedReader(Paths.get(file.getAbsolutePath()))) {
                    String statement;
                    while ((statement = br.readLine()) != null) {
                        try {
                            connection.createStatement().execute(statement);
                            LOG.info("Successfully executed: " + statement);
                        } catch (SQLException e) {
                            LOG.error("Failed in executing: " + statement);
                            LOG.error("Reason: " + e.getMessage());
                            success = false;
                            break;
                        }
                    }
                } catch (IOException e) {
                }
            }
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
            if (success) {
                LOG.info("Committing all transactions.");
                connection.commit();
                File dest = new File(properties.getProperty(FilerUtil.SUCCESS) +
                        File.separator + format.format(new Date()) + "_" + ADHOC_FILENAME);
                LOG.info("Moving " + ADHOC_FILENAME + " to success directory.");
                FileUtils.copyFile(adhocFile, dest);
            } else {
                LOG.info("Rolling back all transactions.");
                connection.rollback();
                File dest = new File(properties.getProperty(FilerUtil.FAILURE) +
                        File.separator + format.format(new Date()) + "_" + ADHOC_FILENAME);
                LOG.info("Moving " + ADHOC_FILENAME + " to failure directory.");
                FileUtils.copyFile(adhocFile, dest);
            }
            FilerUtil.setupStagingDir(stagingDir);

        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        LOG.info("Ending MS SQL Server uploader");
    }
}
