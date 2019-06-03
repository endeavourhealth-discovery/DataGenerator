package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
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
            LOG.info("");
            LOG.error("Error in reading config.properties " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        try {

            File stagingDir = new File(properties.getProperty(FilerConstants.STAGING));
            FilerUtil.setupStagingDir(stagingDir);

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(properties.getProperty(FilerConstants.LOCATION));
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending Subscriber Server uploader");
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
                    System.exit(0);
                }

                for (RemoteFile file : list) {
                    if (file.getFilename().endsWith(".zip") &&
                            !file.getFilename().equalsIgnoreCase(MainAdhoc.ADHOC_FILENAME)) {
                        String remoteFilePath = file.getFullPath();
                        LOG.info("Downloading file: " + file.getFilename());
                        InputStream inputStream = sftp.getFile(remoteFilePath);
                        File dest = new File(stagingDir.getAbsolutePath() + File.separator + file.getFilename());
                        Files.copy(inputStream, dest.toPath());
                        inputStream.close();
                        LOG.info("Deleting file: " + file.getFilename() + " from SFTP server.");
                        sftp.deleteFile(remoteFilePath);
                    }
                }
                sftp.close();
            } catch (Exception e) {
                LOG.info("");
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            File[] files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            FilerUtil.decryptFiles(files, properties);

            files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            ArrayList<String> locations = new ArrayList<>();
            for (File file : files) {
                LOG.info("Deflating zip file: " + file.getName());
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

                Connection con = FilerUtil.getConnection(properties);
                String keywordEscapeChar = con.getMetaData().getIdentifierQuoteString();
                LOG.info("Database connection established.");

                boolean success = true;
                FileInputStream stream = null;
                for (File file : files) {
                    stream = new FileInputStream(file);
                    byte[] bytes = IOUtils.toByteArray(stream);
                    con = FilerUtil.getConnection(properties);
                    con.setAutoCommit(false);
                    LOG.trace("Filing " + bytes.length + "b from file " + file.getName() + " into SQL Server");
                    try {
                        RemoteServerFiler.file(con, keywordEscapeChar, batchSize, bytes);
                    } catch (Exception e) {
                        success = false;
                    }
                    stream.close();
                    file.delete();
                }
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                File source = new File(sourceDir);
                File sourceFile = new File(source.getAbsolutePath() + ".zip");
                String filename = sourceFile.getName();
                if (success) {
                    File dest = new File(properties.getProperty(FilerConstants.SUCCESS) +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to success directory.");
                    FileUtils.copyFile(sourceFile, dest);
                } else {
                    File dest = new File(properties.getProperty(FilerConstants.FAILURE) +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to failure directory.");
                    FileUtils.copyFile(sourceFile, dest);
                }
                FileUtils.deleteDirectory(source);
                FileUtils.forceDelete(sourceFile);
            }
        } catch (Exception e) {
            LOG.info("");
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            LOG.info("");
            System.exit(-1);
        }

        LOG.info("Ending Subscriber Server uploader");
    }
}
