package org.endeavourhealth.scheduler;

import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.job.TransferEncryptedFilesToSftp;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.Connection;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class MainResend {

    private static final Logger LOG = LoggerFactory.getLogger(MainResend.class);

    public static void main(String[] args) throws Exception {

        ConfigManager.Initialize("data-generator");

        if (args.length != 2) {
            LOG.info("Application requires 2 parameters.");
            LOG.info("Parameter 1: Extract ID");
            LOG.info("Parameter 2: Date in yyyyMMdd format");
            return;
        }

        int extractId = 0;
        try {
            extractId = Integer.parseInt(args[0]);
            LOG.debug("extractId:" + extractId);

            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            format.parse(args[1]);
            String filename = extractId + "_" + args[1];

            ExtractConfig config = ExtractCache.getExtractConfig(extractId);

            String housekeep = config.getFileLocationDetails().getHousekeep();
            if (!housekeep.endsWith(File.separator)) {
                housekeep += File.separator;
            }

            LOG.debug("housekeep:" + housekeep);

            //retrieve files for resending
            List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForResending(extractId, filename);
            if (toProcess.size() == 0) {
                LOG.error("There are no transactions associated with the Extract ID = " + extractId + " and Date: " + args[1]);
                return;
            }

            filename = toProcess.get(0).getFilename();
            File[] files = MainResend.getFilesFromDirectory(housekeep, filename.substring(0, filename.indexOf(".")));
            if (toProcess.size() != files.length) {
                LOG.error("Files needed for this Extract ID do not match the available files stored in housekeeping.");
                return;
            }

            ArrayList<String> filesList = new ArrayList();
            for (File file : files) {
                filesList.add(file.getName());
            }

            boolean match = true;
            for (FileTransactionsEntity entry : toProcess) {
                if (!filesList.contains(entry.getFilename())) {
                    match = false;
                    break;
                }
            }
            if (!match) {
                LOG.error("Files needed for this Extract ID do not match the available files stored in housekeeping.");
                return;
            }

            Connection connection = null;
            try {

                TransferEncryptedFilesToSftp transfer = new TransferEncryptedFilesToSftp();
                ConnectionDetails details = transfer.setExtractConfigSftpConnectionDetails(config);
                connection = transfer.openSftpConnection(details);

                for (FileTransactionsEntity entry : toProcess) {
                    filename = entry.getFilename();
                    transfer.uploadFileToSftp(connection,
                            housekeep + filename,
                            config.getFileLocationDetails().getDestination() + filename);

                    LOG.info(filename + " was sent.");
                }
            } catch (Exception e) {
                LOG.error("Error encountered during SFTP. " + e.getMessage());
            } finally {
                if (connection != null) {
                    connection.close();
                }
            }
        } catch (NumberFormatException e) {
            LOG.error("Extract ID should be numeric. " + e.getMessage());
        } catch (ParseException e) {
            LOG.error("Date parameter does not match yyyyMMdd format. " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Error encountered in resending files for Extract ID = " + extractId + " " + e.getMessage());
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
}
