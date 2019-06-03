package org.endeavourhealth.filer.util;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.cms.CMSEnvelopedData;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.KeyTransRecipientInformation;
import org.bouncycastle.cms.RecipientInformation;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.filer.models.FilerConstants;
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
import java.util.Properties;

public class FilerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FilerUtil.class);

    public static Properties initialize() throws Exception {
        Properties properties = new Properties();
        String path = "config.properties";
        InputStream stream = FilerUtil.class.getClassLoader().getResourceAsStream(path);
        properties.load(stream);
        return properties;
    }

    public static void setupStagingDir(File dir) throws Exception {
        FileUtils.deleteDirectory(dir);
        if (!dir.exists()) {
            dir.mkdirs();
        } else {
            FileUtils.deleteDirectory(dir);
            dir.mkdirs();
        }
    }

    public static SftpUtil setupSftp(Properties properties) {

        ConnectionDetails sftpCon = new ConnectionDetails();
        sftpCon.setHostname(properties.getProperty(FilerConstants.HOSTNAME));
        sftpCon.setPort(Integer.valueOf(properties.getProperty(FilerConstants.PORT)));
        sftpCon.setUsername(properties.getProperty(FilerConstants.USERNAME));
        try {
            sftpCon.setClientPrivateKey(FileUtils.readFileToString(new File(properties.getProperty(FilerConstants.KEY))));
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

    public static void decryptFiles(File[] files, Properties properties) {
        try {
            Security.addProvider(new BouncyCastleProvider());

            char[] keystorePassword = properties.getProperty(FilerConstants.PASSWORD).toCharArray();
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(new FileInputStream(properties.getProperty(FilerConstants.P12)), keystorePassword);

            char[] keyPassword = properties.getProperty(FilerConstants.PASSWORD).toCharArray();
            PrivateKey key = (PrivateKey) keystore.getKey(properties.getProperty(FilerConstants.ALIAS), keyPassword);

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

    public static Connection getConnection(Properties properties) throws SQLException, ClassNotFoundException {
        Class.forName(properties.getProperty(FilerConstants.JDBC));
        Properties props = new Properties();
        props.setProperty("user", properties.getProperty(FilerConstants.DB_USERNAME));
        props.setProperty("password", properties.getProperty(FilerConstants.DB_PASSWORD));
        return DriverManager.getConnection(properties.getProperty(FilerConstants.URL), props);
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

