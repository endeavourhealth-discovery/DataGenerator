package org.endeavourhealth.scheduler;

import org.endeavourhealth.scheduler.job.EncryptFiles;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;

public class MainDecrypt {


    private static final Logger LOG = LoggerFactory.getLogger(MainDecrypt.class);

    public static void main(String[] args) {

        if (args.length != 5) {
            LOG.error("Application requires 5 parameters.");
            LOG.error("Parameter 1: P12 file.");
            LOG.error("Parameter 2: Password of the P12 file.");
            LOG.error("Parameter 3: Alias of the private key.");
            LOG.error("Parameter 4: Password of the private key");
            LOG.error("Parameter 5: File that needs to be decrypted.");
            return;
        }

        try {
            char[] keystorePassword = args[1].toCharArray();
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(EncryptFiles.class.getClassLoader().getResourceAsStream(args[0]), keystorePassword);

            char[] keyPassword = args[3].toCharArray();
            PrivateKey key = (PrivateKey) keystore.getKey(args[2], keyPassword);

            File file = new File(args[4]);
            boolean value = PgpEncryptDecrypt.decryptFile(file, key);

        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
