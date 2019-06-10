package org.endeavourhealth.filer.util;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

public class FilerUtil {

    private static final Logger LOG = LoggerFactory.getLogger(FilerUtil.class);

    public static Properties initialize() throws Exception {
        Properties properties = new Properties();
        File jarPath = new File(FilerUtil.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        String propertiesPath = jarPath.getParentFile().getAbsolutePath();
        try {
            //load config.properties where the jar is located
            FileInputStream stream = new FileInputStream(propertiesPath + File.separator + "config.properties");
            properties.load(stream);
        } catch (Exception ex) {
            //load config.properties from classes
            try {
                InputStream stream = FilerUtil.class.getClassLoader().getResourceAsStream("config.properties");
                properties.load(stream);
            } catch (Exception e) {
                throw e;
            }
        }
        return properties;
    }

    public static void setupDirectories(File staging, File success, File failure) throws Exception {
        FileUtils.deleteDirectory(staging);
        staging.mkdirs();
        if (!success.exists()) {
            success.mkdirs();
        }
        if (!failure.exists()) {
            failure.mkdirs();
        }
    }

    public static SftpUtil setupSftp(Properties properties) throws Exception {

        ConnectionDetails sftpCon = new ConnectionDetails();
        sftpCon.setHostname(properties.getProperty(FilerConstants.HOSTNAME));
        sftpCon.setPort(Integer.valueOf(properties.getProperty(FilerConstants.PORT)));
        sftpCon.setUsername(properties.getProperty(FilerConstants.USERNAME));
        try {
            sftpCon.setClientPrivateKey(FileUtils.readFileToString(new File(properties.getProperty(FilerConstants.KEY))));
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

    public static void decryptFiles(File[] files, Properties properties) throws Exception {
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
            LOG.error("Unable to decrypt file/s " + e.getMessage());
            throw e;
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

    public static File createSummaryFiles(File source, ArrayList<String> lSuccess, ArrayList<String> lFailures) throws Exception {
        FileUtils.forceDelete(source);
        String name = source.getAbsolutePath();
        name = name.replace("Data","Results");
        File results = new File(name);
        ZipFile zipFile = new ZipFile(results);
        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
        if (lSuccess.size() > 0) {
            File success = new File(results.getParent() + File.separator + "success.txt");
            FileUtils.writeLines(success, lSuccess);
            zipFile.addFile(success, parameters);
            FileUtils.forceDelete(success);
        }
        if (lFailures.size() > 0) {
            File failure = new File(results.getParent() + File.separator + "failure.txt");
            FileUtils.writeLines(failure, lFailures);
            zipFile.addFile(failure, parameters);
            FileUtils.forceDelete(failure);
        }
        return results;
    }
}

