package org.endeavourhealth.scheduler.util;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.cms.*;
import org.bouncycastle.cms.jcajce.JceCMSContentEncryptorBuilder;
import org.bouncycastle.cms.jcajce.JceKeyTransEnvelopedRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipient;
import org.bouncycastle.cms.jcajce.JceKeyTransRecipientInfoGenerator;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.OutputEncryptor;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PgpEncryptDecrypt {

    public static void encryptFile(File file, X509Certificate encryptionCertificate, String provider)
            throws CertificateEncodingException, CMSException, IOException {

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
        }
    }

    public static void decryptFile(File file, PrivateKey decryptionKey) throws CMSException, IOException {

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
        }
    }

    public static void main(String[] args) throws Exception {

//        File file = new File("C://Temp//sample.csv");
//        FileInputStream fis = new FileInputStream(file);
//        String data = IOUtils.toString(fis, "UTF-8");
//        System.out.println(data);
//
//        String provider = "BC";
//        Security.addProvider(new BouncyCastleProvider());
//        CertificateFactory certFactory= CertificateFactory.getInstance("X.509", provider);
//
//        X509Certificate certificate = (X509Certificate) certFactory.generateCertificate(
//                new FileInputStream("C://Temp//Baeldung.cer"));
//
//        PgpEncryptDecrypt.encryptFile(file, certificate, provider);
//        fis = new FileInputStream(file);
//        data = IOUtils.toString(fis, "UTF-8");
//        System.out.println(data);


//        char[] keystorePassword = "password".toCharArray();
//        char[] keyPassword = "password".toCharArray();
//
//        KeyStore keystore = KeyStore.getInstance("PKCS12");
//        keystore.load(new FileInputStream("C://Temp//Baeldung.p12"), keystorePassword);
//        PrivateKey key = (PrivateKey) keystore.getKey("baeldung", keyPassword);
    }
}
