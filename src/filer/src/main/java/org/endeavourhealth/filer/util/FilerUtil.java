package org.endeavourhealth.filer.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.cms.CMSEnvelopedData;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.KeyTransRecipientInformation;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

public class FilerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FilerUtil.class);
    private static final String JDBC_CLASS_ENV_VAR = "CONFIG_JDBC_CLASS";
    private static final String JDBC_URL_ENV_VAR = "CONFIG_JDBC_URL";
    private static final String JDBC_USER_ENV_VAR = "CONFIG_JDBC_USERNAME";
    private static final String JDBC_PASSWORD_ENV_VAR = "CONFIG_JDBC_PASSWORD";

    public static void initialize(String args[]) {
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
    }

    public static void setupStagingDir(File dir) throws Exception {
        if (!dir.exists()) {
            dir.mkdirs();
        } else {
            File[] files = dir.listFiles();
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
    }

    public static SftpUtil setupSftp(String args[]) {

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

        return sftp;
    }

    public static void decryptFiles(File[] files, String args[]) {
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
    }

    public static Connection getMSSqlServerConnection() throws SQLException, ClassNotFoundException {
        Map<String, String> envVars = System.getenv();
        Class.forName(envVars.getOrDefault(JDBC_CLASS_ENV_VAR, "com.microsoft.sqlserver.jdbc.SQLServerDriver"));
        Properties props = new Properties();
        props.setProperty("user", envVars.getOrDefault(JDBC_USER_ENV_VAR,""));
        props.setProperty("password", envVars.getOrDefault(JDBC_PASSWORD_ENV_VAR, ""));
        return DriverManager.getConnection(envVars.getOrDefault(JDBC_URL_ENV_VAR,""), props);
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

    public static File[] getFilesFromDirectory(String directory, String extension) {
        final String str = extension;
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().endsWith(str);
            }
        };
        return new File(directory).listFiles(fileFilter);
    }
}

