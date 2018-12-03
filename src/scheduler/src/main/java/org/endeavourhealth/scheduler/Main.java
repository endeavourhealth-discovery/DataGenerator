package org.endeavourhealth.scheduler;

import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.datasharingmanagermodel.models.database.ProjectEntity;
import org.endeavourhealth.scheduler.cache.ExtractCache;
import org.endeavourhealth.scheduler.cache.PlainJobExecutionContext;
import org.endeavourhealth.scheduler.job.*;
import org.endeavourhealth.scheduler.json.ExtractDefinition.ExtractConfig;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Scheduler buildCohortScheduler;
    private static Scheduler generateDataScheduler;
    private static Scheduler zipFilesScheduler;
    private static Scheduler encryptFilesScheduler;
    private static Scheduler moveFilesToSFTPScheduler;
    private static Scheduler housekeepFilesScheduler;

    public static final String FILE_JOB_GROUP = "fileJobGroup";
    public static final String ZIP_FILES_JOB = "zipFiles";
    public static final String ENCRYPT_FILES_JOB = "encryptFiles";
    public static final String SFTP_FILES_JOB = "sftpFiles";
    public static final String HOUSEKEEP_FILES_JOB = "housekeepFiles";

    public static void main(String[] args) throws Exception {

        ConfigManager.Initialize("data-generator");

        LOG.info("Checking for extractions");

        List<ExtractEntity> extractsToProcess = new ArrayList<>();
        List<ExtractEntity> allExtracts = ExtractEntity.getAllExtracts();

        for (ExtractEntity extract : allExtracts) {
            ExtractConfig config = ExtractCache.getExtractConfig(extract.getExtractId());
            System.out.println("Checking project status for extract : " + extract.getExtractId()
                    + ", projectId : " + config.getProjectId());
            if (ProjectEntity.checkProjectIsActive(config.getProjectId())) {
                System.out.println("project exists and is active, adding...");
                extractsToProcess.add(extract);
            } else {
                System.out.println("no active project exists, rejecting");
            }
        }

        if (extractsToProcess.size() == 0) {
            LOG.info("No extracts to process. Exiting.");
            return;
        }

        if (args.length == 0) {
            // Run the whole process
            generateData(extractsToProcess);
            return;

        }

        String step = args[0];

        if (step.equals("buildCohort")) {
            buildCohort(false, extractsToProcess);
        }

        if (step.equals("getData")) {
            boolean limitCols = false;
            if (args.length == 2) {
                if (args[1].equals("limit")) {
                    limitCols = true;
                }
            }
            getData(false, limitCols, extractsToProcess);
        }

        if (step.equals("zipFiles")){
            zipFiles(false, extractsToProcess);
        }

        if (step.equals("encryptFiles")) {
            encryptFiles(false, extractsToProcess);
        }

        if (step.equals("moveFiles")) {
            moveFilesToSFTP(false, extractsToProcess);
        }

        if (step.equals("housekeepFiles")) {
            housekeepFiles(false, extractsToProcess);
        }
    }

    private static void generateData(List<ExtractEntity> extractsToProcess) throws Exception {

        LOG.info("Running full process");

        buildCohort(true, extractsToProcess);
        getData(true, false, extractsToProcess);
        zipFiles(true, extractsToProcess);
        encryptFiles(true, extractsToProcess);
        moveFilesToSFTP(true, extractsToProcess);
        housekeepFiles(true, extractsToProcess);

        Thread.sleep(1000);
        zipFilesScheduler.start();
        Thread.sleep(1000);
        encryptFilesScheduler.start();
        Thread.sleep(1000);
        moveFilesToSFTPScheduler.start();
        Thread.sleep(1000);
        housekeepFilesScheduler.start();

        //TODO implementation needed to determine if everything is done
        //TODO testing job scheduling for 100s
        Thread.sleep(100000);
        if (buildCohortScheduler != null) {
            buildCohortScheduler.shutdown();
        }

        if (generateDataScheduler != null) {
            generateDataScheduler.shutdown();
        }

        if (zipFilesScheduler != null) {
            zipFilesScheduler.shutdown();
        }

        if (encryptFilesScheduler != null) {
            encryptFilesScheduler.shutdown();
        }

        if (moveFilesToSFTPScheduler != null) {
            moveFilesToSFTPScheduler.shutdown();
        }

        if (housekeepFilesScheduler != null) {
            housekeepFilesScheduler.shutdown();
        }
    }

    public static void buildCohort(boolean isScheduled, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Building cohort information");
        if (isScheduled) {
            JobDetail buildCohortJob = JobBuilder.newJob(BuildCohort.class).build();

            //TODO determine timing
            //Fire at 1am everyday
            //Trigger buildCohortTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 0 1 * * ?"))
            //        .build();
            //TODO run job only once
            Trigger buildCohortTrigger = TriggerBuilder.newTrigger()
                    .startNow()
                    .build();

            buildCohortScheduler = new StdSchedulerFactory().getScheduler();
            buildCohortScheduler.getContext().put("extractsToProcess", extractsToProcess);
            buildCohortScheduler.start();
            buildCohortScheduler.scheduleJob(buildCohortJob, buildCohortTrigger);

        } else {
            BuildCohort buildCohort = new BuildCohort();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            buildCohort.execute(context);
        }
    }

    private static void getData(boolean isScheduled, boolean limitCols, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Generating CSV data files");
        if (isScheduled) {
            JobDetail generateDataJob = JobBuilder.newJob(GenerateData.class).build();

            //TODO determine timing
            //Fire at 2am everyday
            //Trigger generateDataTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 0 2 * * ?"))
            //        .build();
            //TODO run job only once
            Trigger generateDataTrigger = TriggerBuilder.newTrigger()
                    .startNow()
                    .build();

            generateDataScheduler = new StdSchedulerFactory().getScheduler();
            generateDataScheduler.getContext().put("extractsToProcess", extractsToProcess);
            generateDataScheduler.start();
            generateDataScheduler.scheduleJob(generateDataJob, generateDataTrigger);

        } else {
            GenerateData generateData = new GenerateData();
            generateData.setLimitCols(limitCols);
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            generateData.execute(context);
        }
    }

    private static void zipFiles(boolean isScheduled, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Zipping the files");
        if (isScheduled) {
            JobDetail zipFilesJob =
                    JobBuilder.newJob(ZipCsvFiles.class).withIdentity
                            (JobKey.jobKey(ZIP_FILES_JOB, FILE_JOB_GROUP)).build();

            //TODO determine timing
            //Fire every 10 minutes every hour between 03am and 06am, of every day
            //Trigger zipFilesTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 0/10 3-6 ? * * *"))
            //        .build();
            //TODO temporarily run job every 10 seconds
            Trigger zipFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                    .build();

            zipFilesScheduler = new StdSchedulerFactory().getScheduler();
            zipFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            zipFilesScheduler.scheduleJob(zipFilesJob, zipFilesTrigger);

        } else {
            ZipCsvFiles zipFiles = new ZipCsvFiles();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            zipFiles.execute(context);
        }
    }

    private static void encryptFiles(boolean isScheduled, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Encrypting the files");
        if (isScheduled) {
            JobDetail encryptFilesJob =
                    JobBuilder.newJob(EncryptFiles.class).withIdentity(
                            JobKey.jobKey(ENCRYPT_FILES_JOB, FILE_JOB_GROUP)).build();

            //TODO determine timing
            //Fire every 10 minutes starting at minute :05, every hour between 03am and 06am, of every day
            //Trigger encryptFilesTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 5/10 3-6 ? * * *"))
            //        .build();
            //TODO temporarily run job every 10 seconds
            Trigger encryptFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                    .build();

            encryptFilesScheduler = new StdSchedulerFactory().getScheduler();
            encryptFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            encryptFilesScheduler.scheduleJob(encryptFilesJob, encryptFilesTrigger);

        } else {
            EncryptFiles encryptFiles = new EncryptFiles();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            encryptFiles.execute(context);
        }
    }

    private static void moveFilesToSFTP(boolean isScheduled, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Transferring encrypted files to SFTP");
        if (isScheduled) {
            JobDetail moveFilesToSFTPJob =
                    JobBuilder.newJob(TransferEncryptedFilesToSftp.class).withIdentity(
                            JobKey.jobKey(SFTP_FILES_JOB, FILE_JOB_GROUP)).build();

            //TODO determine timing
            //Fire every 10 minutes starting at minute :10, every hour between 03am and 06am, of every day
            //Trigger moveFilesToSFTPTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 10/10 3-6 ? * * *"))
            //        .build();
            //TODO temporarily run job every 10 seconds
            Trigger moveFilesToSFTPTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                    .build();

            moveFilesToSFTPScheduler = new StdSchedulerFactory().getScheduler();
            moveFilesToSFTPScheduler.getContext().put("extractsToProcess", extractsToProcess);
            moveFilesToSFTPScheduler.scheduleJob(moveFilesToSFTPJob, moveFilesToSFTPTrigger);

        } else {
            TransferEncryptedFilesToSftp sendFilesSFTP = new TransferEncryptedFilesToSftp();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            sendFilesSFTP.execute(context);
        }
    }

    private static void housekeepFiles(boolean isScheduled, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Housekeeping encrypted files");
        if (isScheduled) {
            JobDetail housekeepFilesJob =
                    JobBuilder.newJob(HousekeepFiles.class).withIdentity(
                            JobKey.jobKey(HOUSEKEEP_FILES_JOB, FILE_JOB_GROUP)).build();

            //TODO determine timing
            //Fire every 10 minutes starting at minute :15, every hour between 03am and 06am, of every day
            //Trigger housekeepFilesTrigger = TriggerBuilder.newTrigger()
            //        .withSchedule(CronScheduleBuilder.cronSchedule("0 15/10 3-6 ? * * *"))
            //        .build();
            //TODO temporarily run job every 10 seconds
            Trigger housekeepFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                    .build();

            housekeepFilesScheduler = new StdSchedulerFactory().getScheduler();
            housekeepFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            housekeepFilesScheduler.scheduleJob(housekeepFilesJob, housekeepFilesTrigger);

        } else {
            HousekeepFiles housekeepFiles = new HousekeepFiles();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            housekeepFiles.execute(context);
        }
    }
}
