package org.endeavourhealth.scheduler;

import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.job.TransferEncryptedFilesToSftp;
import org.endeavourhealth.scheduler.json.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.Connection;
import org.endeavourhealth.scheduler.util.ConnectionDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.util.List;

public class MainResend {

    private static final Logger LOG = LoggerFactory.getLogger(MainResend.class);

    public static void main(String[] args) {

        if (args.length == 0) {
            LOG.error("Extract ID is required.");
            return;
        }

        int extractId = 0;
        try {
            extractId = Integer.parseInt(args[0]);
            LOG.debug("extractId:" + extractId);

            ExtractConfig config = ExtractCache.getExtractConfig(extractId);

            String housekeep = config.getFileLocationDetails().getHousekeep();
            if (!housekeep.endsWith(File.separator)) {
                housekeep += File.separator;
            }

            LOG.debug("housekeep:" + housekeep);

            //retrieve files for resending
            List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForResending(extractId);
            if (toProcess.size() == 0) {
                LOG.error("There are no transactions associated with Extract ID = " + extractId);
                return;
            }

            String filename = toProcess.get(0).getFilename();
            File[] files = MainResend.getFilesFromDirectory(housekeep, filename.substring(0, filename.indexOf(".")));
            if (toProcess.size() != files.length) {
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
                if (connection != null)
                    connection.close();
            }
        } catch (NumberFormatException e) {
            LOG.error("Extract ID should be numeric. " + e.getMessage());
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
