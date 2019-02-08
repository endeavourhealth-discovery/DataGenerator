package org.endeavourhealth.scheduler.job;

import org.endeavourhealth.scheduler.MainScheduledExtract;
import org.endeavourhealth.scheduler.cache.PlainJobExecutionContext;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@DisallowConcurrentExecution
@PersistJobDataAfterExecution
public class Organizer implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(Organizer.class);

    @Override
    public void execute(JobExecutionContext context) {

        try {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            ExtractEntity extract = (ExtractEntity) dataMap.get("extract");
            LOG.info(extract.getExtractId() + ":" +extract.getExtractName());

            List<ExtractEntity> extractsToProcess = new ArrayList<ExtractEntity>();
            extractsToProcess.add(extract);

            PlainJobExecutionContext jobContext = new PlainJobExecutionContext();
            jobContext.put("extractsToProcess", extractsToProcess);

            BuildCohort buildCohort = new BuildCohort();
            buildCohort.execute(jobContext);

            GenerateData generateData = new GenerateData();
            generateData.execute(jobContext);

            ZipCsvFiles zipFiles = new ZipCsvFiles();
            zipFiles.execute(jobContext);

            EncryptFiles encryptFiles = new EncryptFiles();
            encryptFiles.execute(jobContext);

            //TODO: Temporarily commented out
            //TransferEncryptedFilesToSftp sendFilesSFTP = new TransferEncryptedFilesToSftp();
            //sendFilesSFTP.execute(jobContext);

            //HousekeepFiles housekeepFiles = new HousekeepFiles();
            //housekeepFiles.execute(jobContext);

        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Unknown error encountered in Organizer handling. " + e.getMessage());
        }

        MainScheduledExtract.todaysJobs--;

    }
}
