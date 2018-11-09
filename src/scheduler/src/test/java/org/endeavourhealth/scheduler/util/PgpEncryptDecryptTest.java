package org.endeavourhealth.scheduler.util;

import org.apache.commons.io.IOUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.Before;
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

import static org.junit.Assert.*;

public class PgpEncryptDecryptTest {

    private ClassLoader classLoader = null;
    private X509Certificate certificate = null;
    private final String provider = "BC";
    private CertificateFactory certFactory = null;
    private PrivateKey key = null;

    @Before
    public void setUp() throws Exception {
        classLoader = PgpEncryptDecrypt.class.getClassLoader();
        Security.addProvider(new BouncyCastleProvider());
        Path path = Paths.get(classLoader.getResource("sample.cer").toURI());
        certFactory = CertificateFactory.getInstance("X.509", provider);
        certificate = (X509Certificate) certFactory.generateCertificate(new FileInputStream(path.toFile()));

        char[] keystorePassword = "password".toCharArray();
        char[] keyPassword = "password".toCharArray();
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        path = Paths.get(classLoader.getResource("sample.p12").toURI());
        keystore.load(new FileInputStream(path.toFile()), keystorePassword);
        key = (PrivateKey) keystore.getKey("sample", keyPassword);
    }

    @Test()
    public void fullCycle()  {

        try {
            Path path = Paths.get(classLoader.getResource("sample.csv").toURI());
            File file = path.toFile();
            FileInputStream fis = new FileInputStream(file);
            String originalData = IOUtils.toString(fis, "UTF-8");

            boolean value = PgpEncryptDecrypt.encryptFile(file, certificate, provider);
            assertTrue(value);
            fis = new FileInputStream(file);
            String encryptedData = IOUtils.toString(fis, "UTF-8");

            assertNotEquals(originalData, encryptedData);

            value = PgpEncryptDecrypt.decryptFile(file, key);
            assertTrue(value);
            fis = new FileInputStream(file);
            String decyptedData = IOUtils.toString(fis, "UTF-8");

            assertEquals(originalData, decyptedData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test()
    public void encryptFileNotFound() {
        try {
            File file = new File("does_not_exists.file");
            PgpEncryptDecrypt.encryptFile(file, certificate, provider);
        } catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Error encountered in file handling."));
        }
    }

}