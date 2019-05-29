package org.endeavourhealth.filer;

import com.amazonaws.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.cms.CMSEnvelopedData;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.KeyTransRecipientInformation;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.filer.util.ConnectionDetails;
import org.endeavourhealth.filer.util.RemoteFile;
import org.endeavourhealth.filer.util.SftpUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class Main {

    private static final String JDBC_CLASS_ENV_VAR = "CONFIG_JDBC_CLASS";
    private static final String JDBC_URL_ENV_VAR = "CONFIG_JDBC_URL";
    private static final String JDBC_USER_ENV_VAR = "CONFIG_JDBC_USERNAME";
    private static final String JDBC_PASSWORD_ENV_VAR = "CONFIG_JDBC_PASSWORD";
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static final int batchSize = 50;

    public static void main(String[] args) {

        LOG.info("Starting MS SQL Server uploader");

        if (args == null || args.length != 9) {
            LOG.error("Invalid number of parameters.");
            System.exit(-1);
        } else {
            for (int i = 0; i < 9; i++) {
                if (StringUtils.isEmpty(args[i])) {
                    LOG.error("Parameter " + (i + 1) + " is required." );
                    System.exit(-1);
                }
            }
        }

        LOG.info("");
        LOG.info("Staging Directory : " + args[0]);
        LOG.info("Hostname          : " + args[1]);
        LOG.info("Port              : " + args[2]);
        LOG.info("Username          : " + args[3]);
        LOG.info("SFTP Location     : " + args[4]);
        LOG.info("Key File          : " + args[5]);
        LOG.info("");
        LOG.info("P12 file          : " + args[6]);
        LOG.info("Alias             : " + args[7]);
        LOG.info("Key File          : " + args[8]);
        LOG.info("");

        try {

            File staging_dir = new File(args[0]);
            if (!staging_dir.exists()) {
                staging_dir.mkdirs();
            } else {
                File[] files = staging_dir.listFiles();
                if (files.length > 0) {
                    LOG.info("");
                    LOG.info("Staging directory is not empty.");
                    for (File file : files) {
                        if (file.isFile()) {
                            LOG.info("Deleting the file: " + file.getName());
                            file.delete();
                        }
                        if (file.isDirectory()) {
                            LOG.info("Deleting the directory: " + file.getName());
                            FileUtils.deleteDirectory(file);
                        }
                    }
                    LOG.info("");
                }
            }

            ConnectionDetails sftpCon = new ConnectionDetails();
            sftpCon.setHostname(args[1]);
            sftpCon.setPort(Integer.valueOf(args[2]));
            sftpCon.setUsername(args[3]);
            try {
                sftpCon.setClientPrivateKey(FileUtils.readFileToString(new File(args[5])));
                sftpCon.setClientPrivateKeyPassword("");
            } catch (IOException e) {
                LOG.info("");
                LOG.error("Unable to read client private key file." + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            SftpUtil sftp = new SftpUtil(sftpCon);
            try {
                sftp.open();
                LOG.info("");
                LOG.info("SFTP connection established");
                LOG.info("");
                sftp.close();
            } catch (Exception e) {
                LOG.info("");
                LOG.error("Unable to connect to the SFTP server." + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            try {
                sftp.open();
                List<RemoteFile> list = sftp.getFileList(args[4]);
                if (list.size() == 0) {
                    LOG.info("SFTP server location is empty.");
                    LOG.info("Ending MS SQL Server uploader");
                    System.exit(0);
                }
                for (RemoteFile file : list) {
                    String remoteFilePath = file.getFullPath();
                    LOG.info("Downloading file: " + file.getFilename());
                    InputStream inputStream = sftp.getFile(remoteFilePath);
                    File dest = new File(staging_dir.getAbsolutePath() + File.separator + file.getFilename());
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

            File[] files = getFilesFromDirectory(staging_dir.getAbsolutePath(), ".zip");
            try {
                Security.addProvider(new BouncyCastleProvider());

                char[] keystorePassword = args[8].toCharArray();
                KeyStore keystore = KeyStore.getInstance("PKCS12");
                keystore.load(new FileInputStream(args[6]), keystorePassword);

                char[] keyPassword = args[8].toCharArray();
                PrivateKey key = (PrivateKey) keystore.getKey(args[7], keyPassword);

                for (File file : files) {
                    boolean decrypted = decryptFile(file, key);
                    if (decrypted) {
                        LOG.info("File: " + file.getName() + " decrypted.");
                    } else {
                        LOG.error("Unable to decrypt file: " + file.getName());
                        LOG.error("Deleting file: " + file.getName());
                        file.delete();
                    }
                }
            } catch (Exception e) {
                LOG.info("");
                LOG.error("Unable to decrypt file/s " + e.getMessage());
                LOG.info("");
                System.exit(-1);
            }

            files = getFilesFromDirectory(staging_dir.getAbsolutePath(), ".zip");
            ArrayList<String> locations = new ArrayList<String>();
            for (File file : files) {
                LOG.info("Deflating zip file: " + file.getName());
                ZipFile zipFile = new ZipFile(file);
                Enumeration<? extends ZipEntry> entries = zipFile.entries();
                while(entries.hasMoreElements()){
                    ZipEntry entry = entries.nextElement();
                    if(entry.isDirectory()){
                        String destPath = staging_dir.getAbsolutePath() + File.separator + entry.getName();
                        File dir = new File(destPath);
                        dir.mkdirs();
                    } else {
                        String destPath = staging_dir.getAbsolutePath() + File.separator + file.getName().substring(0, file.getName().length() - 4);
                        if (!locations.contains(destPath)) {
                            locations.add(destPath);
                        }
                        File dir = new File(destPath);
                        dir.mkdirs();
                        destPath += File.separator + entry.getName();
                        LOG.info("File: " + destPath + " extracted.");
                        try(InputStream inputStream = zipFile.getInputStream(entry); FileOutputStream outputStream = new FileOutputStream(destPath)) {
                            int data = inputStream.read();
                            while(data != -1){
                                outputStream.write(data);
                                data = inputStream.read();
                            }
                        }
                    }
                }
            }

            for (String sourceDir : locations) {
                files = getFilesFromDirectory(sourceDir, ".zip");
                LOG.info("Files in source directory: " + files.length);

                Connection con = Main.getMSSqlServerConnection();
                String keywordEscapeChar = con.getMetaData().getIdentifierQuoteString();
                LOG.info("Database connection established.");

                FileInputStream stream = null;
                for (File file : files) {
                    stream = new FileInputStream(file);
                    byte[] bytes = IOUtils.toByteArray(stream);
                    con = Main.getMSSqlServerConnection();
                    LOG.trace("Filing " + bytes.length + "b from file " + file.getName() + " into SQL Server");
                    SQLServerFiler.file(con, keywordEscapeChar, batchSize, bytes);
                    stream.close();
                    file.delete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        LOG.info("Ending MS SQL Server uploader");
    }

    private static Connection getMSSqlServerConnection() throws SQLException, ClassNotFoundException {
        Map<String, String> envVars = System.getenv();
        Class.forName(envVars.getOrDefault(JDBC_CLASS_ENV_VAR, "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
        Properties props = new Properties();
        props.setProperty("user", envVars.getOrDefault(JDBC_USER_ENV_VAR,""));
        props.setProperty("password", envVars.getOrDefault(JDBC_PASSWORD_ENV_VAR, ""));
        return DriverManager.getConnection(envVars.getOrDefault(JDBC_URL_ENV_VAR,""), props);
    }

    private static File[] getFilesFromDirectory(String directory, String extension) {
        final String str = extension;
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().endsWith(str);
            }
        };
        return new File(directory).listFiles(fileFilter);
    }

    private static boolean decryptFile(File file, PrivateKey decryptionKey) {

        LOG.debug("File:" + file +
                ", PrivateKey:" + decryptionKey);

        FileOutputStream output = null;
        try {
            byte[] encryptedData = org.apache.commons.io.IOUtils.toByteArray(new FileInputStream(file));
            if (encryptedData != null && decryptionKey != null) {

                CMSEnvelopedData envelopedData = new CMSEnvelopedData(encryptedData);
                Collection<RecipientInformation> recipients = envelopedData.getRecipientInfos().getRecipients();
                KeyTransRecipientInformation recipientInfo = (KeyTransRecipientInformation) recipients.iterator().next();
                JceKeyTransRecipient recipient = new JceKeyTransEnvelopedRecipient(decryptionKey);

                output = new FileOutputStream(file);
                output.write(recipientInfo.getContent(recipient));
                output.flush();

                LOG.info("File decryption was successful.");
                return true;
            }
        } catch (IOException e) {
            LOG.error("Error encountered in file handling. " + e.getMessage());
        } catch (CMSException e) {
            LOG.error("Error encountered in decryption handling. " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Unknown error encountered in decryption handling. " + e.getMessage());
        } finally {
            try {
                if (output != null) {
                    output.close();
                }
            } catch (IOException e) {
                LOG.error("Error encountered in file handling. " + e.getMessage());
            }
        }

        LOG.info("File decryption failed.");
        return false;
    }
}
