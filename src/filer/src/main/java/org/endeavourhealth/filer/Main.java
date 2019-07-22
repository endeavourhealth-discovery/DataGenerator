package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FileUtils;
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
            System.exit(-1);
        }

        File stagingDir = new File(properties.getProperty(FilerConstants.STAGING));
        File successDir = new File(properties.getProperty(FilerConstants.SUCCESS));
        File failureDir = new File(properties.getProperty(FilerConstants.FAILURE));

        try {
            FilerUtil.setupDirectories(stagingDir, successDir, failureDir);

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            ArrayList<String> zipFiles = new ArrayList<>();
            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(properties.getProperty(FilerConstants.INCOMING));
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending Subscriber Server uploader");
                    System.exit(0);
                }

                for (RemoteFile file : list) {
                    if (file.getFilename().endsWith(".zip") &&
                            !file.getFilename().equalsIgnoreCase(MainAdhoc.ADHOC_FILENAME)) {
                        String name = file.getFilename().substring(0, (file.getFilename().length() - 4));
                        zipFiles.add(name);
                    }
                }
                if (zipFiles.size() == 0) {
                    LOG.info("SFTP server location contains no valid zip file.");
                    LOG.info("Ending Subscriber Server Server uploader");
                    System.exit(0);
                }
                Arrays.sort(zipFiles.toArray());
                for (RemoteFile file : list) {
                    for (String name : zipFiles) {
                        if (file.getFilename().startsWith(name) &&
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
                }
                sftp.close();
            } catch (Exception e) {
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                System.exit(-1);
            }

            File[] files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            FilerUtil.decryptFiles(files, properties);

            files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            ArrayList<String> locations = new ArrayList<>();

            Arrays.sort(files);
            for (int i = 0; i < files.length; i++) {
                File file = files[i];
                LOG.info("Merging zip file: " + file.getName());
                ZipFile zipFile = new ZipFile(file);
                String name = file.getAbsolutePath().substring(0, (file.getAbsolutePath().length() - 4));
                File merge = new File(name + "_merge.zip");
                zipFile.mergeSplitFiles(merge);
                FileUtils.forceDelete(file);
                FileUtils.copyFile(merge, file);
                files[i] = file;
            }

            Arrays.sort(files);
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

            Arrays.sort(locations.toArray());
            for (String sourceDir : locations) {
                files = FilerUtil.getFilesFromDirectory(sourceDir, ".zip");
                LOG.info("Files in source directory: " + files.length);

                Connection con = FilerUtil.getConnection(properties);
                String keywordEscapeChar = con.getMetaData().getIdentifierQuoteString();
                LOG.info("Database connection established.");
                con.close();

                boolean success = true;
                int nSuccess = 0;
                int nFailures = 0;
                ArrayList<String> lSuccess = new ArrayList();
                ArrayList<String> lFailures = new ArrayList();
                FileInputStream stream = null;
                Arrays.sort(files);
                for (File file : files) {
                    stream = new FileInputStream(file);
                    byte[] bytes = IOUtils.toByteArray(stream);
                    LOG.trace("Filing " + bytes.length + "b from file " + file.getName() + " into DB Server");
                    try {
                        RemoteServerFiler.file(file.getName().substring(24, 60), failureDir.getAbsolutePath(),
                                properties, keywordEscapeChar, batchSize, bytes);
                        nSuccess++;
                        lSuccess.add(file.getName().substring(24, 60));
                    } catch (Exception e) {
                        nFailures++;
                        success = false;
                        lFailures.add(file.getName().substring(24,60) + "," + e.getMessage());
                    }
                    stream.close();
                    FileUtils.forceDelete(file);
                }
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                File source = new File(sourceDir);
                File sourceFile = new File(source.getAbsolutePath() + ".zip");
                String filename = sourceFile.getName();
                LOG.info("Completed processing: " + filename);
                LOG.info("Successfully filed: " + nSuccess);
                LOG.info("Unsuccessfully filed: " + nFailures);
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
                LOG.info("Generating summary file: " + sourceFile.getName().replace("Data","Results"));
                File summary = FilerUtil.createSummaryFiles(sourceFile, lSuccess, lFailures);
                sftp.open();
                LOG.info("Uploading summary file: " + summary.getName());
                sftp.put(summary.getAbsolutePath(), properties.getProperty(FilerConstants.RESULTS));
                sftp.close();
                FileUtils.deleteDirectory(source);
                FileUtils.forceDelete(summary);
            }
            FileUtils.deleteDirectory(stagingDir);
            stagingDir.mkdirs();
        } catch (Exception e) {
            LOG.error("Unhandled exception occurred. " + e.getMessage());
            try {
                FilerUtil.setupDirectories(stagingDir, successDir, failureDir);
            } catch(Exception ex) {
            }
            System.exit(-1);
        }

        LOG.info("Ending Subscriber Server uploader");
        System.exit(0);
    }
}
