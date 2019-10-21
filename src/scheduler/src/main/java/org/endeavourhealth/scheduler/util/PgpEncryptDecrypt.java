package org.endeavourhealth.scheduler.util;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.cms.*;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipientInfoGenerator;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OutputEncryptor;
import org.endeavourhealth.scheduler.job.EncryptFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;

public class PgpEncryptDecrypt {

    private static final Logger LOG = LoggerFactory.getLogger(PgpEncryptDecrypt.class);

    public static boolean encryptFile(File file,
                                      X509Certificate encryptionCertificate,
                                      String provider) {

        FileOutputStream output = null;
        try {
            byte[] data = IOUtils.toByteArray(new FileInputStream(file));
            if (data != null && encryptionCertificate != null) {

                CMSEnvelopedDataGenerator cmsEnvelopedDataGenerator = new CMSEnvelopedDataGenerator();
                JceKeyTransRecipientInfoGenerator jceKey = new JceKeyTransRecipientInfoGenerator(encryptionCertificate);
                cmsEnvelopedDataGenerator.addRecipientInfoGenerator(jceKey);
                CMSTypedData msg = new CMSProcessableByteArray(data);
                OutputEncryptor encrypt =
                        new JceCMSContentEncryptorBuilder(CMSAlgorithm.AES256_CBC).setProvider(provider).build();
                CMSEnvelopedData cmsEnvelopedData = cmsEnvelopedDataGenerator.generate(msg, encrypt);

                output = new FileOutputStream(file);
                output.write(cmsEnvelopedData.getEncoded());
                output.flush();

                LOG.info("File encryption was successful.");
                return true;
            }
        } catch (IOException e) {
            LOG.error("Error encountered in file handling. " + e.getMessage());
        } catch (CertificateEncodingException e) {
            LOG.error("Error encountered in certificate handling. " + e.getMessage());
        } catch (CMSException e) {
            LOG.error("Error encountered in encryption handling. " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Unknown error encountered in encryption handling. " + e.getMessage());
        } finally {
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }

        LOG.info("File encryption failed.");
        return false;
    }

    public static boolean decryptFile(File file, PrivateKey decryptionKey) {

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
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }

        LOG.info("File decryption failed.");
        return false;
    }

    public static void main(String args[]) {

        X509Certificate certificate = null;
        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", "BC");
            certificate =
                    (X509Certificate) certFactory.generateCertificate(new FileInputStream(new File(args[0])));
        } catch (FileNotFoundException e) {
            LOG.error("Certificate file not found. " + e.getMessage());
        } catch (CertificateException e) {
            LOG.error("Error encountered in certificate generation. " + e.getMessage());
        } catch (NoSuchProviderException e) {
            LOG.error("Error encountered in certificate provider. " + e.getMessage());
        }

        File file = new File(args[0]);
        boolean success = PgpEncryptDecrypt.encryptFile(file, certificate, "BC");
        if (success) {

            LOG.info("File: " + args[0] + " encrypted.");
        }
    }
}
