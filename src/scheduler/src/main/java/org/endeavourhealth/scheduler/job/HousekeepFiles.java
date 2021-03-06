package org.endeavourhealth.scheduler.job;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.FileUtils;
import org.endeavourhealth.scheduler.Main;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.endeavourhealth.scheduler.util.JobUtil;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

public class HousekeepFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(HousekeepFiles.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
                if (JobUtil.isJobRunning(jobExecutionContext,
                        new String[]{
                                Main.ZIP_FILES_JOB,
                                Main.ENCRYPT_FILES_JOB,
                                Main.SFTP_FILES_JOB},
                        Main.FILE_JOB_GROUP)) {

                    LOG.info("Conflicting job is still running");
                    return;
                }
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
        } catch (Exception e) {
            LOG.error("Unknown error encountered in housekeep handling. " + e.getMessage());
        }
        LOG.info("Beginning of housekeeping files");

        Main main = Main.getInstance();
        for (ExtractEntity entity : extractsToProcess) {

            LOG.info("Extract ID: " + entity.getExtractId());
            try {
                ExtractConfig config = ExtractCache.getExtractConfig(entity.getExtractId());
                // LOG.info(config.getName());

                String location = config.getFileLocationDetails().getSource();
                if (!location.endsWith(File.separator)) {
                    location += File.separator;
                }

                String housekeep = config.getFileLocationDetails().getHousekeep();
                if (!housekeep.endsWith(File.separator)) {
                    housekeep += File.separator;
                }

                LOG.debug("Location: " + location);
                LOG.debug("Housekeep: " + housekeep);

                //retrieve files for housekeeping
                List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForHousekeeping(entity.getExtractId());
                if (toProcess == null || toProcess.size() == 0) {
                    LOG.info("No file/s for housekeeping");
                } else {
                    File src = null;
                    File dest = null;

                    for (FileTransactionsEntity entry : toProcess) {

                        src = new File(location + entry.getFilename());
                        dest = new File(housekeep + entry.getFilename());

                        try {
                            FileUtils.copyFile(src, dest);
                            System.gc();
                            Thread.sleep(1000);
                            FileDeleteStrategy.FORCE.delete(src);
                            LOG.info("File: " + src.getName() + " moved to " + housekeep);
                            try {
                                //update the file's encryption date
                                entry.setHousekeepingDate(new Timestamp(System.currentTimeMillis()));
                                FileTransactionsEntity.update(entry);
                                LOG.info("File: " + src.getName() + " record updated");
                            } catch (Exception e) {
                                LOG.error("Exception occurred with using the database: " + e);
                            }
                        } catch (IOException e) {
                            main.errorEncountered(main.incrememtErrorCount());
                            LOG.error("Error encountered in moving the file to housekeep. " + e.getMessage());
                        }
                    }
                    main.endScheduler(main.incrememtExtractsProcessed());
                }
            } catch (Exception e) {
                main.errorEncountered(main.incrememtErrorCount());
                LOG.error("Exception occurred with using the database. " + e);
            }
        }
        LOG.info("End of housekeeping files");
    }
}