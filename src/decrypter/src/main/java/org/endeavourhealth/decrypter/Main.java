package org.endeavourhealth.decrypter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
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
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.security.*;
import java.util.Collection;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 4) {
            LOG.info("Application requires 4 parameters.");
            LOG.info("Parameter 1: P12 file");
            LOG.info("Parameter 2: Alias");
            LOG.info("Parameter 3: Password");
            LOG.info("Parameter 4: Main file (.zip) that needs to be decrypted.");
            return;
        }

        try {
            Security.addProvider(new BouncyCastleProvider());

            char[] keystorePassword = args[2].toCharArray();
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(Main.class.getClassLoader().getResourceAsStream(args[0]), keystorePassword);

            char[] keyPassword = args[2].toCharArray();
            PrivateKey key = (PrivateKey) keystore.getKey(args[1], keyPassword);

            File file = new File(args[3]);
            if (!file.getName().contains(".zip")) {
                LOG.info("Parameter 4 needs to be a zip file.");
                return;
            }

            Path path = FileSystems.getDefault().getPath(".").toAbsolutePath();
            String sPath = path.toFile().getAbsolutePath();
            sPath = sPath.substring(0, sPath.length() - 1);
            File destination = new File(sPath);

            File[] files = getFilesFromDirectory(file.getParent(), file.getName().substring(0, file.getName().length() - 2));
            if (files != null && files.length != 0) {
                for (File zipPart : files) {
                    FileUtils.copyFileToDirectory(zipPart, destination, false);
                    LOG.info("Encrypted file: " + file.getAbsolutePath() + " copied to: " + destination.getAbsolutePath());
                }
            }

            File unencrypted = new File(destination.getAbsolutePath() + File.separator + file.getName());
            if (Main.decryptFile(unencrypted, key)) {
                LOG.info(unencrypted.getAbsolutePath() + " was decrypted.");
            } else {
                LOG.info("File decryption failed.");
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private static File[] getFilesFromDirectory(String directory, String prefix) {
        final String str = prefix;
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                return file.getName().startsWith(str);
            }
        };
        return new File(directory).listFiles(fileFilter);
    }

    private static boolean decryptFile(File file, PrivateKey decryptionKey) {

        LOG.debug("File:" + file +
                ", PrivateKey:" + decryptionKey);

        FileOutputStream output = null;
        try {
            byte[] encryptedData = IOUtils.toByteArray(new FileInputStream(file));
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
