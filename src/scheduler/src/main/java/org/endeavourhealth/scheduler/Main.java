package org.endeavourhealth.scheduler;

import org.endeavourhealth.scheduler.cache.PlainJobExecutionContext;
import org.endeavourhealth.scheduler.job.*;
import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Scheduler buildCohortScheduler;
    private static Scheduler generateDataScheduler;
    private static Scheduler zipFilesScheduler;
    private static Scheduler encryptFilesScheduler;
    private static Scheduler moveFilesToSFTPScheduler;
    private static Scheduler housekeepFilesScheduler;

    public static void main(String[] args) throws Exception {

        LOG.info("Checking for extractions");

        List<ExtractEntity> extractsToProcess = ExtractEntity.getAllExtracts();
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

        //TODO implementation needed to determine if everything is done
        //TODO testing job scheduling for 100s
        Thread.sleep(100000);
        if (buildCohortScheduler != null) {
            buildCohortScheduler.shutdown();
        }

        if (generateDataScheduler != null) {
            generateDataScheduler.shutdown();
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
            //TODO temporarily run job every 5 seconds
            Trigger buildCohortTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
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
            //TODO temporarily run job every 10 seconds
            Trigger generateDataTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
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
            JobDetail zipFilesJob = JobBuilder.newJob(ZipCsvFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 20 seconds
            Trigger zipFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/20 * * * * ?"))
                    .build();

            zipFilesScheduler = new StdSchedulerFactory().getScheduler();
            zipFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            zipFilesScheduler.start();
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
            JobDetail encryptFilesJob = JobBuilder.newJob(EncryptFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 25 seconds
            Trigger encryptFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/25 * * * * ?"))
                    .build();

            encryptFilesScheduler = new StdSchedulerFactory().getScheduler();
            encryptFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            encryptFilesScheduler.start();
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
            JobDetail moveFilesToSFTPJob = JobBuilder.newJob(TransferEncryptedFilesToSftp.class).build();

            //TODO determine timing
            //TODO temporarily run job every 30 seconds
            Trigger moveFilesToSFTPTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/30 * * * * ?"))
                    .build();

            moveFilesToSFTPScheduler = new StdSchedulerFactory().getScheduler();
            moveFilesToSFTPScheduler.getContext().put("extractsToProcess", extractsToProcess);
            moveFilesToSFTPScheduler.start();
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
            JobDetail housekeepFilesJob = JobBuilder.newJob(HousekeepFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 35 seconds
            Trigger housekeepFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/35 * * * * ?"))
                    .build();

            housekeepFilesScheduler = new StdSchedulerFactory().getScheduler();
            housekeepFilesScheduler.getContext().put("extractsToProcess", extractsToProcess);
            housekeepFilesScheduler.start();
            housekeepFilesScheduler.scheduleJob(housekeepFilesJob, housekeepFilesTrigger);

        } else {
            HousekeepFiles housekeepFiles = new HousekeepFiles();
            PlainJobExecutionContext context = new PlainJobExecutionContext();
            context.put("extractsToProcess", extractsToProcess);
            housekeepFiles.execute(context);
        }
    }
}
