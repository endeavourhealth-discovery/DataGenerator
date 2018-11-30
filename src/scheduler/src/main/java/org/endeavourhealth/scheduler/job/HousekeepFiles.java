package org.endeavourhealth.scheduler.job;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.endeavourhealth.scheduler.models.database.FileTransactionsEntity;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class HousekeepFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(HousekeepFiles.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        List<ExtractEntity> extractsToProcess = null;
        try {
            if (jobExecutionContext.getScheduler() != null) {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.getScheduler().getContext().get("extractsToProcess");
            } else {
                extractsToProcess = (List<ExtractEntity>) jobExecutionContext.get("extractsToProcess");
            }
        } catch (SchedulerException e) {
            LOG.error("Unknown error encountered in housekeep handling. " + e.getMessage());
        }
        for (ExtractEntity entity : extractsToProcess) {

            LOG.info("Extract ID:" + entity.getExtractId());
            try {
                ExtractConfig config = ExtractCache.getExtractConfig(entity.getExtractId());
                String location = config.getFileLocationDetails().getSource();
                if (!location.endsWith(File.separator)) {
                    location += File.separator;
                }

                String housekeep = config.getFileLocationDetails().getHousekeep();
                if (!housekeep.endsWith(File.separator)) {
                    housekeep += File.separator;
                }

                LOG.debug("location:" + location);
                LOG.debug("housekeep:" + housekeep);

                //retrieve files for housekeeping
                List<FileTransactionsEntity> toProcess = FileTransactionsEntity.getFilesForHousekeeping(entity.getExtractId());
                if (toProcess == null || toProcess.size() == 0) {
                    LOG.info("No file/s for housekeeping.");
                } else {
                    File src = null;
                    File dest = null;

                    for (FileTransactionsEntity entry : toProcess) {

                        src = new File(location + entry.getFilename());
                        dest = new File(housekeep + entry.getFilename());

                        try {
                            FileUtils.moveFile(src, dest);

                            LOG.info("File:" + src.getName() + " moved to " + housekeep);

                            //update the file's encryption date
                            entry.setHousekeepingDate(new Timestamp(System.currentTimeMillis()));
                            FileTransactionsEntity.update(entry);

                            LOG.info("File:" + src.getName() + " record updated.");

                        } catch (IOException e) {
                            LOG.error("Error encountered in moving the file. " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                LOG.error("Error encountered in housekeeping files. " + e.getMessage());
            }
        }
    }
}
