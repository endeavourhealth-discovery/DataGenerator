package org.endeavourhealth.scheduler.util;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.cms.*;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipientInfoGenerator;
import org.bouncycastle.operator.OutputEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Collection;

public class PgpEncryptDecrypt {

    private static final Logger LOG = LoggerFactory.getLogger(PgpEncryptDecrypt.class);

    public static boolean encryptFile(File file,
                                      X509Certificate encryptionCertificate,
                                      String provider) {

        LOG.debug("File:" + file +
                // ", X509Certificate:" + encryptionCertificate +
                ", Provider:" + provider);

        try {
            byte[] data = IOUtils.toByteArray(new FileInputStream(file));
            if (data != null && encryptionCertificate != null) {

                CMSEnvelopedDataGenerator cmsEnvelopedDataGenerator = new CMSEnvelopedDataGenerator();
                JceKeyTransRecipientInfoGenerator jceKey = new JceKeyTransRecipientInfoGenerator(encryptionCertificate);
                cmsEnvelopedDataGenerator.addRecipientInfoGenerator(jceKey);
                CMSTypedData msg = new CMSProcessableByteArray(data);
                OutputEncryptor encrypt =
                        new JceCMSContentEncryptorBuilder(CMSAlgorithm.AES128_CBC).setProvider(provider).build();
                CMSEnvelopedData cmsEnvelopedData = cmsEnvelopedDataGenerator.generate(msg, encrypt);

                FileOutputStream output = new FileOutputStream(file);
                output.write(cmsEnvelopedData.getEncoded());
                output.flush();
                output.close();

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
        }

        LOG.info("File encryption failed.");
        return false;
    }

    public static boolean decryptFile(File file, PrivateKey decryptionKey) {

        LOG.debug("File:" + file +
                ", PrivateKey:" + decryptionKey);

        try {
            byte[] encryptedData = IOUtils.toByteArray(new FileInputStream(file));
            if (encryptedData != null && decryptionKey != null) {

                CMSEnvelopedData envelopedData = new CMSEnvelopedData(encryptedData);
                Collection<RecipientInformation> recipients = envelopedData.getRecipientInfos().getRecipients();
                KeyTransRecipientInformation recipientInfo = (KeyTransRecipientInformation) recipients.iterator().next();
                JceKeyTransRecipient recipient = new JceKeyTransEnvelopedRecipient(decryptionKey);

                FileOutputStream output = new FileOutputStream(file);
                output.write(recipientInfo.getContent(recipient));
                output.flush();
                output.close();

                LOG.info("File decryption was successful.");
                return true;
            }
        } catch (IOException e) {
            LOG.error("Error encountered in file handling. " + e.getMessage());
        } catch (CMSException e) {
            LOG.error("Error encountered in decryption handling. " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Unknown error encountered in decryption handling. " + e.getMessage());
        }

        LOG.info("File decryption was successful.");
        return false;
    }
}
