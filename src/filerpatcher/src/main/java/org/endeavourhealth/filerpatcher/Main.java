package org.endeavourhealth.filerpatcher;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.filerpatcher.util.ConnectionDetails;
import org.endeavourhealth.filerpatcher.util.FilerPatcherConstants;
import org.endeavourhealth.filerpatcher.util.RemoteFile;
import org.endeavourhealth.filerpatcher.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        LOG.info("Starting Filer Patcher");
        if (args.length < 2) {
            LOG.error("Invalid number of parameters.");
            System.exit(-1);
        }
        LOG.info("Parameters:");
        LOG.info("Updated Filer name: " + args[0]);
        LOG.info("Existing Filer Location: " + args[1]);


        if (!args[0].startsWith("filer-1.0-SNAPSHOT-jar-with-dependencies")) {
            LOG.error("Parameter 1 does not specify a valid filer filename.");
            System.exit(-1);
        }

        if (!args[0].endsWith(".jar")) {
            LOG.error("Parameter 1 does not specify a valid filer jar file.");
            System.exit(-1);
        }

        File location = new File(args[1]);
        if (!location.isDirectory()) {
            LOG.error("Parameter 2 is not a valid directory.");
            System.exit(-1);
        }

        ArrayList<File> files = new ArrayList<>(Arrays.asList(location.listFiles()));
        boolean found = false;
        File filerJar = null;
        for (File file : files) {
            if (file.getName().equals("filer-1.0-SNAPSHOT-jar-with-dependencies.jar")) {
                found = true;
                filerJar = file;
                break;
            }
        }

        if (!found) {
            LOG.error("Directory parameter does not contain filer-1.0-SNAPSHOT-jar-with-dependencies.jar.");
            System.exit(-1);
        }

        Properties properties = null;
        try {
            properties = initialize();
        } catch (Exception e) {
            LOG.error("Error in reading config.properties. " + e.getMessage());
            System.exit(-1);
        }

        File stagingDir = new File(properties.getProperty(FilerPatcherConstants.STAGING));
        try {
            FileUtils.forceDelete(stagingDir);
            FileUtils.forceMkdir(stagingDir);
        } catch (Exception e) {
            LOG.error("Error in recreating staging directory. " + e.getMessage());
            System.exit(-1);
        }

        SftpUtil sftp = null;
        try {
            sftp = setupSftp(properties);
        } catch (Exception e) {
            LOG.error("Error in setting up SFTP " + e.getMessage());
            System.exit(-1);
        }


        File downloadedJar = null;
        try {
            sftp.open();
            List<RemoteFile> list = sftp.getFileList(properties.getProperty(FilerPatcherConstants.FILERPATCHER));
            if (list.size() == 0) {
                LOG.info("SFTP server location is empty.");
                LOG.info("Ending Filer Patcher");
                System.exit(0);
            }

            for (RemoteFile file : list) {
                if (file.getFilename().equalsIgnoreCase(args[0])) {
                    String remoteFilePath = file.getFullPath();
                    LOG.info("Downloading file: " + file.getFilename());
                    InputStream inputStream = sftp.getFile(remoteFilePath);
                    downloadedJar = new File(stagingDir.getAbsolutePath() + File.separator + file.getFilename());
                    Files.copy(inputStream, downloadedJar.toPath());
                    LocalDateTime time = file.getLastModified();
                    downloadedJar.setLastModified(new Date().from(time.atZone(ZoneId.systemDefault()).toInstant()).getTime());
                    inputStream.close();
                    LOG.info("Deleting file: " + file.getFilename() + " from SFTP server.");
                    sftp.deleteFile(remoteFilePath);
                }
            }
            sftp.close();
        } catch (Exception e) {
            LOG.error("Error in downloading/deleting files from SFTP server", e);
            System.exit(-1);
        }

        if (downloadedJar != null) {

            Date existingFiler = new Date(filerJar.lastModified());
            Date downloadedFiler = new Date(downloadedJar.lastModified());

            LOG.info("Timestamp of existing filer: " + existingFiler);
            LOG.info("Timestamp of downloaded filer: " + downloadedFiler);

            if (downloadedFiler.after(existingFiler)) {
                File backup = new File(args[1] + File.separator + "filer-1.0-SNAPSHOT-jar-with-dependencies.backup");
                LOG.info("Renaming existing filer to filer-1.0-SNAPSHOT-jar-with-dependencies.backup");
                backup.delete();
                filerJar.renameTo(backup);
                filerJar.delete();
                LOG.info("Updating filer...");
                downloadedJar.renameTo(filerJar);
                LOG.info("Filer jar was updated.");
                LOG.info("Ending Filer Patcher");
                System.exit(0);
            } else {
                LOG.info("Downloaded Filer jar is not newer than the existing Filer jar, updating was cancelled.");
                LOG.info("Ending Filer Patcher");
                System.exit(0);
            }
        } else {
            LOG.info("SFTP server location does not contain the file: " + args[0]);
            LOG.info("Ending Filer Patcher");
            System.exit(0);
        }
    }

    public static Properties initialize() throws Exception {
        Properties properties = new Properties();
        File jarPath = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String propertiesPath = jarPath.getParentFile().getAbsolutePath();
        try {
            //load config.properties where the jar is located
            FileInputStream stream = new FileInputStream(propertiesPath + File.separator + "config.properties");
            properties.load(stream);
        } catch (Exception ex) {
            //load config.properties from classes
            try {
                InputStream stream = Main.class.getClassLoader().getResourceAsStream("config.properties");
                properties.load(stream);
            } catch (Exception e) {
                throw e;
            }
        }
        return properties;
    }

    public static SftpUtil setupSftp(Properties properties) throws Exception {

        ConnectionDetails sftpCon = new ConnectionDetails();
        sftpCon.setHostname(properties.getProperty(FilerPatcherConstants.HOSTNAME));
        sftpCon.setPort(Integer.valueOf(properties.getProperty(FilerPatcherConstants.PORT)));
        sftpCon.setUsername(properties.getProperty(FilerPatcherConstants.USERNAME));
        try {
            sftpCon.setClientPrivateKey(FileUtils.readFileToString(new File(properties.getProperty(FilerPatcherConstants.KEY))));
            sftpCon.setClientPrivateKeyPassword("");
        } catch (IOException e) {
            LOG.error("Unable to read client private key file." + e.getMessage());
            throw e;
        }

        SftpUtil sftp = new SftpUtil(sftpCon);
        try {
            sftp.open();
            LOG.info("SFTP connection established");
            sftp.close();
        } catch (Exception e) {
            LOG.error("Unable to connect to the SFTP server." + e.getMessage());
            throw e;
        }

        return sftp;
    }
}
