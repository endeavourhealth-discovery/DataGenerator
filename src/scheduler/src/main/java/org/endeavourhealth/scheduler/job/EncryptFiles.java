package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
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
import java.sql.Timestamp;
import java.util.List;

public class EncryptFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(EncryptFiles.class);
    private static final String PROVIDER = "BC";

    public void execute(JobExecutionContext jobExecutionContext) {

        List<FileTransactionsEntity> toProcess;
        String location = null;
        try {
            //TODO determine logic to pass or obtain from tables the value/s needed for extractId
            int extractId = 123;
            ExtractConfig config = ExtractCache.getExtractConfig(extractId);
            location = config.getFileLocationDetails().getSource();
            if (!location.endsWith(File.separator)) {
                location += File.separator;
            }
            LOG.debug("location:" + location);

            //retrieve files for encryption
            toProcess = FileTransactionsEntity.getFilesForEncryption(extractId);
            if (toProcess == null || toProcess.size() == 0) {
                LOG.info("No file/s ready for encryption.");
                return;
            }

        } catch (Exception e) {
            LOG.error("Error encountered in extracting data. " + e.getMessage());
            e.printStackTrace();
            return;
        }

        try {

            //create certificate object
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", PROVIDER);
            Path path =
                    Paths.get(EncryptFiles.class.getClassLoader().getResource("endeavour.cer").toURI());
            X509Certificate certificate =
                    (X509Certificate) certFactory.generateCertificate(new FileInputStream(path.toFile()));

            for (FileTransactionsEntity entry : toProcess) {

                File file = new File(location + entry.getFilename());
                boolean success = PgpEncryptDecrypt.encryptFile(file, certificate, PROVIDER);

                if (success) {

                    LOG.info("File:" + file.getName() + " encrypted.");

                    //update the file's encryption date
                    entry.setEncryptDate(new Timestamp(System.currentTimeMillis()));
                    FileTransactionsEntity.update(entry);

                    LOG.info("File:" + file.getName() + " record updated.");

                } else {
                    LOG.error("File:" + file.getName() + " failed encryption.");
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
        } catch (Exception e) {
            LOG.error("Unknown error encountered in encryption handling. " + e.getMessage());
        }
    }
}
