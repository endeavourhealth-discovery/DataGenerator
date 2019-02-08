package org.endeavourhealth.scheduler;

import org.apache.commons.lang3.time.DateUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.datasharingmanagermodel.models.database.DataProcessingAgreementEntity;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.job.*;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class MainScheduledExtract {

    private static final Logger LOG = LoggerFactory.getLogger(MainScheduledExtract.class);
    public static final String FILE_JOB_GROUP = "fileJobGroup";
    public static final String ORGANIZER_JOB = "organize";
    public static int todaysJobs = 0;

    public static void main(String[] args) {


        List<ExtractEntity> validExtracts = new ArrayList<>();

        try {
            ConfigManager.Initialize("data-generator");

            LOG.info("Checking for extractions");

            List<ExtractEntity> extracts = ExtractEntity.getAllExtracts();
            List<DataProcessingAgreementEntity> results = null;

            for (ExtractEntity extract : extracts) {
                ExtractConfig config = ExtractCache.getExtractConfig(extract.getExtractId());
                LOG.info("Checking project status for extract: " + extract.getExtractId() +
                        ", projectId : " + config.getProjectId());

                results = DataProcessingAgreementEntity.getDataProcessingAgreementsForOrganisation(config.getProjectId());
                //results.add(new DataProcessingAgreementEntity());
                if (results != null && results.size() > 0) {
                    LOG.info("Project exists and is active, adding...");
                    validExtracts.add(extract);
                } else {
                    LOG.info("No active project exists, rejecting...");
                }
            }

            if (validExtracts.size() == 0) {
                LOG.info("No extracts to process. Exiting.");
                return;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        Scheduler mainScheduler = null;
        JobBuilder builder = null;
        JobDetail job = null;
        Trigger trigger = null;

        try {
            mainScheduler = new StdSchedulerFactory().getScheduler();
            mainScheduler.start();
            builder = JobBuilder.newJob(ScheduleAliveJob.class);
            job = builder.build();
            trigger = TriggerBuilder.newTrigger().startNow().withSchedule(
                    SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(1800).repeatForever()).build();
            mainScheduler.scheduleJob(job, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

        for (ExtractEntity extract : validExtracts ) {
            JobDataMap map = new JobDataMap();
            map.put("extract", extract);
            builder = JobBuilder.newJob(Organizer.class);
            builder.withIdentity(JobKey.jobKey(ORGANIZER_JOB, FILE_JOB_GROUP + "_" + extract.getExtractName()));
            builder.usingJobData(map);
            job = builder.build();
            try {
                trigger = TriggerBuilder.newTrigger().withSchedule(
                        CronScheduleBuilder.cronSchedule(extract.getCron())).build();
                Date date = mainScheduler.scheduleJob(job, trigger);
                LOG.info("Extract Id:" + extract.getExtractId() + " to run at: " + date);
                if (DateUtils.isSameDay(date, Calendar.getInstance().getTime())) {
                    todaysJobs++;
                }
            } catch (RuntimeException e) {
                LOG.error("Failed to schedule Extract Id: " + extract.getExtractId() + ". Reason: " + e.getMessage());
            } catch (SchedulerException e) {
                LOG.error("Failed to schedule Extract Id: " + extract.getExtractId() + ". Reason: " + e.getMessage());
            }
        }

        LOG.info("Scheduled jobs for today: "+todaysJobs);

        while(true) {
            if (todaysJobs == 0) {
                try {
                    LOG.info("Scheduler shutting down....");
                    mainScheduler.shutdown();
                    mainScheduler = null;
                    System.exit(0);
                } catch (SchedulerException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }
}