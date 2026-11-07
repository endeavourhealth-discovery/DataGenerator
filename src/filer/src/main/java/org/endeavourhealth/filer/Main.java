package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.filer.models.FilerConstants;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.RemoteFile;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static int batchSize = 5000;
    private static Map<String, ArrayList> columnsMap = new ConcurrentHashMap<>();
    private static Map<String, ArrayList> pksMap = new ConcurrentHashMap<>();
    private static Map<String, Boolean> identityTables = new ConcurrentHashMap<>();

    public static void main(String[] args) {

        /*
        try {
            if (FilerUtil.isRunning("filer-1.0-SNAPSHOT-jar-with-dependencies") > 1) {
                LOG.info("filer is already running");
                System.exit(-1);
            }
        }
        catch (Exception e)
        {
            LOG.error("Unhandled exception (isRunning)", e);
            System.exit(-1);
        }
        */

        Boolean chkLock = FilerUtil.checkIfLockFileExists("filer");
        if (chkLock.booleanValue() == true) {
            LOG.error("filer is already running");
            System.exit(0);
        }

        LOG.info("Starting Subscriber Server uploader");
        TimeTracker.start("remote_filer");
        TimeTracker.logAction("initialise");

        //optional first parameter allows overriding the filing batch size
        if (args.length > 0) {
            try {
                int paramSize = Integer.parseInt(args[0]);
                if (paramSize < batchSize) {
                    LOG.info("Ignoring batchSize parameter, less than the default 5000. Parameter: " + args[0]);
                } else {
                    batchSize = paramSize;
                }
            } catch (Exception e) {
                LOG.info("Ignoring batchSize parameter, not a valid value: " + args[0]);
            }
        }

        //optional second parameter allows overriding the properties file name
        String propertyFileName = "config.properties";
        if (args.length > 1) {
            propertyFileName = args[1];
        }

        Properties properties = null;
        try {
            properties = FilerUtil.initialize(propertyFileName);
        } catch (Exception e) {
            LOG.error("Error in reading config.properties " + e.getMessage());
            System.exit(-1);
        }


        LOG.info("batchSize:" + batchSize);

        boolean enterprise = Boolean.valueOf(properties.getProperty(FilerConstants.ENTERPRISE, "false"));

        File stagingDir = new File(properties.getProperty(FilerConstants.STAGING));
        File successDir = new File(properties.getProperty(FilerConstants.SUCCESS));
        File failureDir = new File(properties.getProperty(FilerConstants.FAILURE));

        try {

            /*
            Boolean chkLock = FilerUtil.checkIfLockFileExists("filer");
            if (chkLock.booleanValue() == true) {
                LOG.error("filer is already running");
                System.exit(0);
            }
            */

            FilerUtil.setupDirectories(stagingDir, successDir, failureDir);

            TimeTracker.logAction("downloading_files");

            SftpUtil sftp = FilerUtil.setupSftp(properties);
            ArrayList<String> zipFiles = new ArrayList<>();
            ArrayList<String> zipSplitFiles = new ArrayList<>();

            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(properties.getProperty(FilerConstants.INCOMING));
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending Subscriber Server uploader");
                    FilerUtil.deleteLockFile("filer");
                    System.exit(0);
                }

                for (RemoteFile file : list) {
                    if (file.getFilename().endsWith(".zip") &&
                            !file.getFilename().equalsIgnoreCase(MainAdhoc.ADHOC_FILENAME)) {
                        String name = file.getFilename().substring(0, (file.getFilename().length() - 4));
                        zipFiles.add(name);
                    }
                    String[] splitName = file.getFilename().split("\\.");
                    String splitExtension = splitName[1];
                    if (FilerUtil.RegEx(splitExtension,"^(z)[0-9]{2}$").equals(1)) {
                        zipSplitFiles.add(file.getFilename());
                    }
                }
                if (zipFiles.size() == 0) {
                    LOG.info("SFTP server location contains no valid zip file.");
                    LOG.info("Ending Subscriber Server Server uploader");
                    FilerUtil.deleteLockFile("filer");
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

                            // for https://endeavourhealth.atlassian.net/browse/IDD-3
                            // do not delete the files from the sftp server at this point, wait until
                            // they have been sucessfully filed lower down in the method.  At this point,
                            // store a list of the filename downloaded remoteFilePath names, including where
                            // there is multi-part zips with the same prefix which contibute to one filed archive
                            //LOG.info("Deleting file: " + file.getFilename() + " from SFTP server.");
                            //sftp.deleteFile(remoteFilePath);
                        }
                    }
                }
                sftp.close();
            } catch (Exception e) {
                LOG.error("Error in downloading/deleting files from SFTP server", e);
                FilerUtil.deleteLockFile("filer");
                System.exit(-1);
            }

            TimeTracker.logAction("decrypting_files");

            File[] files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            FilerUtil.decryptFiles(files, properties);

            Arrays.sort(files);

            for (int i = 0; i < files.length; i++) {
                File file = files[i];

                TimeTracker.logAction("unzip_files");

                LOG.info("Merging zip file: " + file.getName());
                ZipFile zipFile = new ZipFile(file);
                String name = file.getAbsolutePath().substring(0, (file.getAbsolutePath().length() - 4));
                File merge = new File(name + "_merge.zip");
                try {
                    zipFile.mergeSplitFiles(merge);
                    //FileUtils.forceDelete(file);
                    System.gc();
                    Thread.sleep(1000);
                    Path filepath = file.toPath();
                    Files.delete(filepath);
                    FileUtils.copyFile(merge, file);
                    FileUtils.forceDelete(merge);
                } catch (ZipException ex) {
                    if (ex.getMessage().equalsIgnoreCase("archive not a split zip file")) {
                        LOG.info("File is not multi-part. " + file.getName());
                    } else {
                        throw ex;
                    }
                }

                LOG.info("Deflating zip file: " + file.getName());
                zipFile = new ZipFile(file);
                String destPath = stagingDir.getAbsolutePath() + File.separator + file.getName().substring(0, file.getName().length() - 4);
                File dir = new File(destPath);
                dir.mkdirs();
                zipFile.extractAll(destPath);

                File[] zips = FilerUtil.getFilesFromDirectory(destPath, ".zip");
                LOG.info("Files in source directory: " + zips.length);

                Connection connection = FilerUtil.getConnection(properties);
                LOG.info("Database connection established.");
                String keywordEscapeChar = connection.getMetaData().getIdentifierQuoteString();
                connection.close();

                boolean success = true;
                int nSuccess = 0;
                int nFailures = 0;
                ArrayList<String> lSuccess = new ArrayList();
                ArrayList<String> lFailures = new ArrayList();

                Arrays.sort(zips);

                for (File zip : zips) {
                    TimeTracker.logAction("read_zip_from_disk");
                    FileInputStream fis = new FileInputStream(zip);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    byte[] bytes = IOUtils.toByteArray(bis);
                    bis.close();

                    LOG.trace("Filing " + bytes.length + "b from file " + zip.getName() + " into DB Server");
                    try {
                        if (enterprise) {
                            RemoteEnterpriseFiler.file(zip.getName().substring(24, 60), failureDir.getAbsolutePath(),
                                    properties, keywordEscapeChar, batchSize, bytes, columnsMap, pksMap, identityTables);
                        } else {
                            RemoteServerFiler.file(zip.getName().substring(24, 60), failureDir.getAbsolutePath(),
                                    properties, keywordEscapeChar, batchSize, bytes, columnsMap, pksMap, identityTables);
                        }
                        TimeTracker.logAction("after_batch");
                        nSuccess++;
                        lSuccess.add(zip.getName().substring(24, 60));
                    } catch (Exception e) {
                        TimeTracker.logAction("handling_errors");
                        LOG.error("Exception filing zip " + zip.getName(), e);
                        nFailures++;
                        success = false;
                        if (e.getMessage().contains("SQLServerPreparedStatement")) {
                            if (e.getCause() != null && e.getCause().getCause() != null && e.getCause().getCause().getMessage() != null) {
                                if (e.getCause().getCause().getMessage().length() < 65535) {
                                    lFailures.add(zip.getName().substring(24, 60) + "," + e.getCause().getCause().getMessage());
                                } else {
                                    lFailures.add(zip.getName().substring(24, 60) + "," + e.getCause().getCause().getMessage().substring(0, 65534));
                                }
                            } else {
                                lFailures.add(zip.getName().substring(24, 60) + "," + e.getMessage());
                            }
                        } else {
                            lFailures.add(zip.getName().substring(24, 60) + "," + e.getMessage());
                        }
                    }

                    FileUtils.forceDelete(zip);
                    TimeTracker.logAction("after_delete_zip");
                }

                TimeTracker.logAction("moving_to_archive");
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
                File source = new File(destPath);
                File sourceFile = new File(source.getAbsolutePath() + ".zip");
                String filename = sourceFile.getName();
                LOG.info("Completed processing: " + filename);
                LOG.info("Successfully filed: " + nSuccess);
                LOG.info("Unsuccessfully filed: " + nFailures);
                if (success) {
                    //TODO: temporarily removed while doing bulk
                    /*
                    File dest = new File(successDir.getAbsolutePath() +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to success directory.");
                    FileUtils.copyFile(sourceFile, dest);
                    */
                } else {
                    //TODO: temporarily removed while doing bulk
                    /*
                    File dest = new File(failureDir.getAbsolutePath() +
                            File.separator + format.format(new Date()) + "_" + filename);
                    LOG.info("Moving " + filename + " to failure directory.");
                    FileUtils.copyFile(sourceFile, dest);
                    */
                }
                TimeTracker.logAction("writing_feedback");
                LOG.info("Generating summary file: " + sourceFile.getName().replace("Data","Results"));
                File summary = FilerUtil.createSummaryFiles(sourceFile, lSuccess, lFailures);
                sftp.open();
                LOG.info("Uploading summary file: " + summary.getName());
                sftp.put(summary.getAbsolutePath(), properties.getProperty(FilerConstants.RESULTS));

                // https://endeavourhealth.atlassian.net/browse/IDD-3
                if (lFailures.size() == 0) {
                    // delete the remote file(s)
                    String remoteFolder = properties.getProperty(FilerConstants.INCOMING);
                    // config folder path usually does not end with a /, so append one if not for correct remote file deletion
                    if (!remoteFolder.endsWith("/")) {
                        remoteFolder = remoteFolder.concat("/");
                    }
                    String remoteFilePath = remoteFolder + sourceFile.getName();
                    LOG.info("Success, deleting: "+remoteFilePath);
                    sftp.deleteFile(remoteFilePath);

                    // delete the associated split files!
                    for (String splitName : zipSplitFiles) {
                        LOG.info(splitName);
                        String zipName = sourceFile.getName().substring(0, (sourceFile.getName().length() - 4));
                        if (splitName.startsWith(zipName)) {
                            remoteFilePath = remoteFolder + splitName;
                            LOG.info("deleting split zip file: " + remoteFilePath);
                            sftp.deleteFile(remoteFilePath);
                        }
                    }
                }

                sftp.close();
                FileUtils.deleteDirectory(source);
                FileUtils.forceDelete(summary);
            }

            FilerUtil.deleteLockFile("filer");

            //FileUtils.deleteDirectory(stagingDir);
            //stagingDir.mkdirs();

        } catch (Exception e) {
            LOG.error("Unhandled exception occurred", e);
            try {
                FilerUtil.setupDirectories(stagingDir, successDir, failureDir);
                FilerUtil.deleteLockFile("filer");
            } catch(Exception ex) {
            }
            System.exit(-1);
        }

        TimeTracker.stop();

        LOG.info("Ending Subscriber Server uploader");
        System.exit(0);
    }

}
