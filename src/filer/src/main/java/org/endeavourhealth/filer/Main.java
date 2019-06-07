package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.filer.models.FilerConstants;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.RemoteFile;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final int batchSize = 50;

    public static void main(String[] args) {

        LOG.info("Starting Subscriber Server uploader");

        Properties properties = null;
        try {
            properties = FilerUtil.initialize();
        } catch (Exception e) {
            LOG.error("Error in reading config.properties " + e.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Server uploader");
            System.exit(-1);
        }

        SlackHelper.setupConfig(properties.getProperty(FilerConstants.NET_PROXY),
                properties.getProperty(FilerConstants.NET_PORT),
                SlackHelper.Channel.RemoteFilerAlerts.getChannelName(),
                "https://hooks.slack.com/services/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");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Starting Subscriber Server uploader");

        try {

            File stagingDir = new File(properties.getProperty(FilerConstants.STAGING));
            File successDir = new File(properties.getProperty(FilerConstants.SUCCESS));
            File failureDir = new File(properties.getProperty(FilerConstants.FAILURE));
            FilerUtil.setupDirectories(stagingDir, successDir, failureDir);

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(properties.getProperty(FilerConstants.INCOMING));
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending Subscriber Server uploader");
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "SFTP server location is empty.");
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Server uploader");
                    System.exit(0);
                }

                boolean zipFound = false;
                for (RemoteFile file : list) {
                    if (file.getFilename().endsWith(".zip") &&
                            !file.getFilename().equalsIgnoreCase(MainAdhoc.ADHOC_FILENAME)) {
                        zipFound = true;
                        break;
                    }
                }
                if (!zipFound) {
                    LOG.info("SFTP server location contains no valid zip file.");
                    LOG.info("Ending Subscriber Server Server uploader");
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "SFTP server location contains no valid zip file.");
                    SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Server uploader");
                    System.exit(0);
                }

                for (RemoteFile file : list) {
                    if (file.getFilename().endsWith(".zip") &&
                            !file.getFilename().equalsIgnoreCase(MainAdhoc.ADHOC_FILENAME)) {
                        String remoteFilePath = file.getFullPath();
                        LOG.info("Downloading file: " + file.getFilename());
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Downloading file: " + file.getFilename());
                        InputStream inputStream = sftp.getFile(remoteFilePath);
                        File dest = new File(stagingDir.getAbsolutePath() + File.separator + file.getFilename());
                        Files.copy(inputStream, dest.toPath());
                        inputStream.close();
                        LOG.info("Deleting file: " + file.getFilename() + " from SFTP server.");
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Deleting file: " + file.getFilename() + " from SFTP server.");
                        sftp.deleteFile(remoteFilePath);
                    }
                }
                sftp.close();
            } catch (Exception e) {
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Error in downloading/deleting files from SFTP server ", e);
                System.exit(-1);
            }

            File[] files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            FilerUtil.decryptFiles(files, properties);

            files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            ArrayList<String> locations = new ArrayList<>();
            for (File file : files) {
                LOG.info("Deflating zip file: " + file.getName());
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Deflating zip file: " + file.getName());
                ZipFile zipFile = new ZipFile(file);
                String destPath = stagingDir.getAbsolutePath() + File.separator + file.getName().substring(0, file.getName().length() - 4);
                if (!locations.contains(destPath)) {
                    locations.add(destPath);
                }
                File dir = new File(destPath);
                dir.mkdirs();
                zipFile.extractAll(destPath);
            }

            for (String sourceDir : locations) {
                files = FilerUtil.getFilesFromDirectory(sourceDir, ".zip");
                LOG.info("Files in source directory: " + files.length);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Files in source directory: " + files.length);

                Connection con = FilerUtil.getConnection(properties);
                String keywordEscapeChar = con.getMetaData().getIdentifierQuoteString();
                LOG.info("Database connection established.");
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Database connection established.");

                boolean success = true;
                int nSuccess = 0;
                int nFailures = 0;
                ArrayList<String> lSuccess = new ArrayList();
                ArrayList<String> lFailures = new ArrayList();
                FileInputStream stream = null;
                for (File file : files) {
                    stream = new FileInputStream(file);
                    byte[] bytes = IOUtils.toByteArray(stream);
                    con = FilerUtil.getConnection(properties);
                    con.setAutoCommit(false);
                    LOG.trace("Filing " + bytes.length + "b from file " + file.getName() + " into DB Server");
                    try {
                        RemoteServerFiler.file(con, keywordEscapeChar, batchSize, bytes);
                        nSuccess++;
                        lSuccess.add(file.getName().substring(24,60));
                    } catch (Exception e) {
                        nFailures++;
                        success = false;
                        lFailures.add(file.getName().substring(24,60));
                    }
                    stream.close();
                    file.delete();
                }
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                File source = new File(sourceDir);
                File sourceFile = new File(source.getAbsolutePath() + ".zip");
                String filename = sourceFile.getName();
                LOG.info("Completed processing: " + filename);
                LOG.info("Successfully filed: " + nSuccess);
                LOG.info("Unsuccessfully filed: " + nFailures);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Completed processing: " + filename);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Successfully filed: " + nSuccess);
                SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Unsuccessfully filed: " + nFailures);
                if (success) {
                    File dest = new File(successDir.getAbsolutePath() +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to success directory.");
                    FileUtils.copyFile(sourceFile, dest);
                } else {
                    File dest = new File(failureDir.getAbsolutePath() +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to failure directory.");
                    FileUtils.copyFile(sourceFile, dest);
                }
                LOG.info("Generating summary file: " + sourceFile.getName());
                File summary = FilerUtil.createSummaryFiles(sourceFile, lSuccess, lFailures);
                sftp.open();
                LOG.info("Uploading summary file: " + sourceFile.getName());
                sftp.put(summary.getAbsolutePath(), properties.getProperty(FilerConstants.RESULTS));
                sftp.close();
                FileUtils.deleteDirectory(source);
                FileUtils.forceDelete(sourceFile);
            }
        } catch (Exception e) {
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Unhandled exception occurred. " , e);
            SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Server uploader");
            System.exit(-1);
        }

        LOG.info("Ending Subscriber Server uploader");
        SlackHelper.sendSlackMessage(SlackHelper.Channel.RemoteFilerAlerts, "Ending Subscriber Server uploader");
        System.exit(0);
    }
}
