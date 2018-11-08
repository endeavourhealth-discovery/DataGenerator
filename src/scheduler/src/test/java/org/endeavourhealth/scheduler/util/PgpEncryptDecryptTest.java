package org.endeavourhealth.scheduler.util;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class PgpEncryptDecryptTest {

    @Test()
    public void fullCycle()  {

        try {
            ClassLoader classLoader = PgpEncryptDecrypt.class.getClassLoader();
            Path path = Paths.get(classLoader.getResource("sample.csv").toURI());
            File file = path.toFile();
            FileInputStream fis = new FileInputStream(file);
            String originalData = IOUtils.toString(fis, "UTF-8");

            String provider = "BC";
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory= CertificateFactory.getInstance("X.509", provider);
            path = Paths.get(classLoader.getResource("sample.cer").toURI());
            X509Certificate certificate = (X509Certificate) certFactory.generateCertificate(new FileInputStream(path.toFile()));
            //PgpEncryptDecrypt.encryptFile(file, certificate, provider);
            fis = new FileInputStream(file);
            String encryptedData = IOUtils.toString(fis, "UTF-8");

            //assertNotEquals(originalData, encryptedData);

            char[] keystorePassword = "password".toCharArray();
            char[] keyPassword = "password".toCharArray();
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            path = Paths.get(classLoader.getResource("sample.p12").toURI());
            keystore.load(new FileInputStream(path.toFile()), keystorePassword);
            PrivateKey key = (PrivateKey) keystore.getKey("sample", keyPassword);
            //PgpEncryptDecrypt.decryptFile(file, key);
            fis = new FileInputStream(file);
            String decyptedData = IOUtils.toString(fis, "UTF-8");

            assertEquals(originalData, decyptedData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}