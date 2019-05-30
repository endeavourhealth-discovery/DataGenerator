package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import net.lingala.zip4j.core.ZipFile;
import org.endeavourhealth.filer.util.FilerUtil;
import org.endeavourhealth.filer.util.RemoteFile;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.*;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final int batchSize = 50;

    public static void main(String[] args) {

        LOG.info("Starting MS SQL Server uploader");

        FilerUtil.initialize(args);

        try {

            File stagingDir = new File(args[0]);
            FilerUtil.setupStagingDir(stagingDir);

            SftpUtil sftp = FilerUtil.setupSftp(args);
            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(args[4]);
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending MS SQL Server uploader");
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
                    LOG.info("Ending MS SQL Server uploader");
                    System.exit(0);
                }

                for (RemoteFile file : list) {
                    String remoteFilePath = file.getFullPath();
                    LOG.info("Downloading file: " + file.getFilename());
                    InputStream inputStream = sftp.getFile(remoteFilePath);
                    File dest = new File(stagingDir.getAbsolutePath() + File.separator + file.getFilename());
                    Files.copy(inputStream, dest.toPath());
                    inputStream.close();
                    LOG.info("Deleting file: " + file.getFilename() + " from SFTP server.");
                    sftp.deleteFile(remoteFilePath);
                }
                sftp.close();
            } catch (Exception e) {
                LOG.info("");
                LOG.error("Error in downloading/deleting files from SFTP server " + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            File[] files = FilerUtil.getFilesFromDirectory(stagingDir.getAbsolutePath(), ".zip");
            FilerUtil.decryptFiles(files, args);

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

                Connection con = FilerUtil.getMSSqlServerConnection();
                String keywordEscapeChar = con.getMetaData().getIdentifierQuoteString();
                LOG.info("Database connection established.");

                FileInputStream stream = null;
                for (File file : files) {
                    stream = new FileInputStream(file);
                    byte[] bytes = IOUtils.toByteArray(stream);
                    con = FilerUtil.getMSSqlServerConnection();
                    con.setAutoCommit(false);
                    LOG.trace("Filing " + bytes.length + "b from file " + file.getName() + " into SQL Server");
                    SQLServerFiler.file(con, keywordEscapeChar, batchSize, bytes);
                    stream.close();
                    file.delete();
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
