package org.endeavourhealth.scheduler.job;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.endeavourhealth.scheduler.Main;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.JobUtil;
import org.endeavourhealth.scheduler.util.PgpEncryptDecrypt;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Timestamp;
import java.util.List;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class EncryptFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(EncryptFiles.class);
    private static final String PROVIDER = "BC";

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
            if (JobUtil.isJobRunning(jobExecutionContext,
                    new String[] {
                            Main.ZIP_FILES_JOB,
                            Main.SFTP_FILES_JOB,
                            Main.HOUSEKEEP_FILES_JOB},
                    Main.FILE_JOB_GROUP)) {

                LOG.info("Conflicting job is still running");
                return;
            }
        } catch (Exception e) {
            LOG.error("Unknown error encountered in encrypt handling. " + e.getMessage());
        }

        X509Certificate certificate = null;
        try {
            Security.addProvider(new BouncyCastleProvider());
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509", PROVIDER);
            certificate =
                    (X509Certificate) certFactory.generateCertificate(
                            EncryptFiles.class.getClassLoader().getResourceAsStream("endeavour.cer"));
        } catch (CertificateException e) {
            LOG.error("Error encountered in certificate generation. " + e.getMessage());
        } catch (NoSuchProviderException e) {
            LOG.error("Error encountered in certificate provider. " + e.getMessage());
        }

        LOG.info("Beginning of encrypting zip files");

        List<FileTransactionsEntity> toProcess;
        String location = null;
        for (ExtractEntity entity : extractsToProcess) {

            LOG.info("Extract ID: " + entity.getExtractId());

            try {
                //TODO determine logic to pass or obtain from tables the value/s needed for extractId
                ExtractConfig config = ExtractCache.getExtractConfig(entity.getExtractId());
                // LOG.info(config.getName());

                location = config.getFileLocationDetails().getSource();
                if (!location.endsWith(File.separator)) {
                    location += File.separator;
                }
                LOG.debug("Location: " + location);

                //retrieve files for encryption
                toProcess = FileTransactionsEntity.getFilesForEncryption(entity.getExtractId());
                if (toProcess == null || toProcess.size() == 0) {
                    LOG.info("No file/s ready for encryption.");
                } else {
                    for (FileTransactionsEntity entry : toProcess) {

                        boolean success = true;
                        if (entry.getFilename().contains(".zip")) {
                            File file = new File(location + entry.getFilename());
                            success = PgpEncryptDecrypt.encryptFile(file, certificate, PROVIDER);
                        }

                        if (success) {

                            if (entry.getFilename().contains(".zip")) {
                                LOG.info("File: " + entry.getFilename() + " encrypted.");
                            }

                            //update the file's encryption date
                            entry.setEncryptDate(new Timestamp(System.currentTimeMillis()));
                            FileTransactionsEntity.update(entry);

                            LOG.info("File: " + entry.getFilename() + " record updated.");

                        } else {
                            LOG.error("File: " + entry.getFilename() + " failed encryption.");
                            return;
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Unknown error encountered in encryption handling. " + e.getMessage());
            }
        }
        LOG.info("End of encrypting zip files");
    }
}