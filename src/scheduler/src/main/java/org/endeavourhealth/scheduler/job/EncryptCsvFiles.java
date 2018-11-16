package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractConfig;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchProviderException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class EncryptCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(EncryptCsvFiles.class);
    private static final String PROVIDER = "BC";

    public void execute(JobExecutionContext jobExecutionContext) {

        String location = null;
        try {
            ExtractConfig config = ExtractCache.getExtractConfig(1);
            location = config.getFileLocationDetails().getSource();
            if (!location.endsWith(File.separator)) {
                location += File.separator;
            }
            LOG.debug("location:" + location);
        } catch (Exception e) {
            LOG.error("Error encountered in extracting configuration. " + e.getMessage());
            return;
        }

        try {
            //TODO logic to determine which files are going to be encrypted
            List<File> files = new ArrayList<File>();

            //TODO remove test code
            files.add(new File(location + "testEncrypt1.txt"));
            files.add(new File(location + "testEncrypt2.txt"));

            CertificateFactory certFactory =
                    certFactory = CertificateFactory.getInstance("X.509", PROVIDER);

            HashMap<File, X509Certificate> sourceCertificateMap = new HashMap<File, X509Certificate>();

            //TODO are we only going to use just one certificate/pk12 pair to encrypt/decrypt
            // or do we handle this in a per client basis; assuming one only for now
            Path path =
                    Paths.get(EncryptCsvFiles.class.getClassLoader().getResource("endeavour.cer").toURI());
            X509Certificate certificate =
                    (X509Certificate) certFactory.generateCertificate(new FileInputStream(path.toFile()));

            //TODO remove test code
            for (File file : files) {
                sourceCertificateMap.put(file, certificate);
            }

            for (File file : sourceCertificateMap.keySet()) {
                boolean success = PgpEncryptDecrypt.encryptFile(file, sourceCertificateMap.get(file), PROVIDER);
                if (success) {
                    LOG.info("File:" + file.getName() + " encrypted.");
                } else {
                    LOG.error("File:" + file.getName() + " encrypted.");
                }
            }
        } catch (CertificateException e) {
            LOG.error("Error encountered in certificate generation. " + e.getMessage());
        } catch (NoSuchProviderException e) {
            LOG.error("Error encountered in certificate provider. " + e.getMessage());
        } catch (URISyntaxException e) {
            LOG.error("Certificate file not found. " + e.getMessage());
        } catch (FileNotFoundException e) {
            LOG.error("Certificate file not found. " + e.getMessage());
        }
    }
}
