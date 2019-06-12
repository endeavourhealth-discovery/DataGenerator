package org.endeavourhealth.scheduler;

import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.security.datasharingmanagermodel.models.database.DataProcessingAgreementEntity;
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

    public static final String FILE_JOB_GROUP = "fileJobGroup";
    public static final String BUILD_COHORT_JOB = "buildCohort";
    public static final String GENERATE_FILES_JOB = "generateFiles";
    public static final String ZIP_FILES_JOB = "zipFiles";
    public static final String ENCRYPT_FILES_JOB = "encryptFiles";
    public static final String SFTP_FILES_JOB = "sftpFiles";
    public static final String HOUSEKEEP_FILES_JOB = "housekeepFiles";

    private static final int ERROR_LIMIT = 20;
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Scheduler mainScheduler = null;

    private int totalExtracts = 0;
    private boolean buildCohortDone = false;
    private boolean generateFilesDone = false;

    private int zipProcessed = 0;
    private int encryptProcessed = 0;
    private int sftpProcessed = 0;
    private int extractsProcessed = 0;
    private int errorCount = 0;

    private static Main instance = null;

    private Main() {
    }

    public static Main getInstance()
    {
        if (instance == null)
            instance = new Main();

        return instance;
    }

    public static void main(String[] args) throws Exception {

        ConfigManager.Initialize("data-generator");

        Main main = Main.getInstance();

        LOG.info("Checking for extractions");

        List<ExtractEntity> extractsToProcess = new ArrayList<>();
        List<ExtractEntity> allExtracts = ExtractEntity.getAllExtracts();

        for (ExtractEntity extract : allExtracts) {
            ExtractConfig config = ExtractCache.getExtractConfig(extract.getExtractId());
            LOG.info("Checking project status for extract : " + extract.getExtractId()
                    + ", projectId : " + config.getProjectId());

            List<DataProcessingAgreementEntity> results = DataProcessingAgreementEntity.getDataProcessingAgreementsForOrganisation(config.getProjectId());
            //results.add(new DataProcessingAgreementEntity());
            if (results != null && results.size() > 0) {
                LOG.info("Project exists and is active, adding...");
                extractsToProcess.add(extract);
            } else {
                LOG.info("No active project exists, rejecting...");
            }
        }

        if (extractsToProcess.size() == 0) {
            LOG.info("No extracts to process. Exiting.");
            return;
        } else {
            main.setTotalExtracts(extractsToProcess.size());
        }

        if (args.length == 0) {
            // Run the whole process
            main.generateData(extractsToProcess);
            return;
        }

        String step = args[0];

        if (step.equals("buildCohort")) {
            main.buildCohort(extractsToProcess);
        }

        if (step.equals("getData")) {
            boolean limitCols = false;
            if (args.length == 2) {
                if (args[1].equals("limit")) {
                    limitCols = true;
                }
            }
            main.getData(limitCols, extractsToProcess);
        }

        if (step.equals("zipFiles")){
            main.zipFiles(extractsToProcess);
        }

        if (step.equals("encryptFiles")) {
            main.encryptFiles(extractsToProcess);
        }

        if (step.equals("moveFiles")) {
            main.moveFilesToSFTP(extractsToProcess);
        }

        if (step.equals("housekeepFiles")) {
            main.housekeepFiles(extractsToProcess);
        }
    }

    private void generateData(List<ExtractEntity> extractsToProcess) throws Exception {

        LOG.info("Running full process");

        JobKey buildCohortJobKey = (JobKey.jobKey(BUILD_COHORT_JOB, FILE_JOB_GROUP));
        JobDetail buildCohortJob =
                JobBuilder.newJob(BuildCohort.class).withIdentity(buildCohortJobKey).storeDurably().build();

        JobKey generateDataJobKey = JobKey.jobKey(GENERATE_FILES_JOB, FILE_JOB_GROUP);
        JobDetail generateDataJob =
                JobBuilder.newJob(GenerateData.class).withIdentity(generateDataJobKey).storeDurably().build();

        JobDetail zipFilesJob =
                JobBuilder.newJob(ZipCsvFiles.class).withIdentity
                        (JobKey.jobKey(ZIP_FILES_JOB, FILE_JOB_GROUP)).build();
        Trigger zipFilesTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                .build();

        JobDetail encryptFilesJob =
                JobBuilder.newJob(EncryptFiles.class).withIdentity(
                        JobKey.jobKey(ENCRYPT_FILES_JOB, FILE_JOB_GROUP)).build();
        Trigger encryptFilesTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                .build();

        JobDetail moveFilesToSFTPJob =
                JobBuilder.newJob(TransferEncryptedFilesToSftp.class).withIdentity(
                        JobKey.jobKey(SFTP_FILES_JOB, FILE_JOB_GROUP)).build();
        Trigger moveFilesToSFTPTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                .build();

        JobDetail housekeepFilesJob =
                JobBuilder.newJob(HousekeepFiles.class).withIdentity(
                        JobKey.jobKey(HOUSEKEEP_FILES_JOB, FILE_JOB_GROUP)).build();
        Trigger housekeepFilesTrigger = TriggerBuilder.newTrigger()
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                .build();

        long counter = 0;

        mainScheduler =  new StdSchedulerFactory().getScheduler();
        mainScheduler.getContext().put("extractsToProcess", extractsToProcess);
        mainScheduler.start();

        mainScheduler.addJob(buildCohortJob, true);
        mainScheduler.triggerJob(buildCohortJobKey);
        while (!buildCohortDone) {
            if (counter == 2147483647) {
                counter = 0;
                LOG.info("Waiting for Build Cohort to complete");
            }
            if (getBuildCohortDone()) {
                LOG.info("Build Cohort complete");
                break;
            }
            counter++;
        }

        counter = 0;
        mainScheduler.addJob(generateDataJob, true);
        mainScheduler.triggerJob(generateDataJobKey);
        while (!generateFilesDone) {
            if (counter == 2147483647) {
                counter = 0;
                LOG.info("Waiting for Generate Data to complete");
            }
            if (getGenerateFilesDone()) {
                LOG.info("Generate Data complete");
                break;
            }
            counter++;
        }

        Thread.sleep(1000);
        mainScheduler.scheduleJob(zipFilesJob, zipFilesTrigger);
        Thread.sleep(1000);
        mainScheduler.scheduleJob(encryptFilesJob, encryptFilesTrigger);
        Thread.sleep(1000);
        mainScheduler.scheduleJob(moveFilesToSFTPJob, moveFilesToSFTPTrigger);
        Thread.sleep(1000);
        mainScheduler.scheduleJob(housekeepFilesJob, housekeepFilesTrigger);
    }

    public void endJob(String job, int processed) {
        if (processed == totalExtracts) {
            if (mainScheduler != null) {
                try {
                    LOG.info("Job: " + job + " is done.");
                    mainScheduler.pauseJob(JobKey.jobKey(job, FILE_JOB_GROUP));
                } catch (SchedulerException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    public void endScheduler(int processed) {
        if (processed == totalExtracts) {
            terminateScheduler();
        }
    }

    public void errorEncountered(int errors) {
        LOG.info("count:" + errors);
        if (errors == ERROR_LIMIT) {
            LOG.error("Application has reached its allowable error count.");
            terminateScheduler();
        }
    }

    private void terminateScheduler() {
        try {
            if (mainScheduler != null) {
                mainScheduler.shutdown();
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private void buildCohort(List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Building cohort information");
        BuildCohort buildCohort = new BuildCohort();
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        buildCohort.execute(context);
    }

    private void getData(boolean limitCols, List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Generating CSV data files");
        GenerateData generateData = new GenerateData();
        generateData.setLimitCols(limitCols);
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        generateData.execute(context);
    }

    private void zipFiles(List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Zipping CSV files");
        ZipCsvFiles zipFiles = new ZipCsvFiles();
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        zipFiles.execute(context);
    }

    private void encryptFiles(List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Encrypting zip files");
        EncryptFiles encryptFiles = new EncryptFiles();
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        encryptFiles.execute(context);
    }

    private void moveFilesToSFTP(List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Transferring encrypted files to SFTP");
        TransferEncryptedFilesToSftp sendFilesSFTP = new TransferEncryptedFilesToSftp();
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        sendFilesSFTP.execute(context);
    }

    private void housekeepFiles(List<ExtractEntity> extractsToProcess) throws Exception {
        LOG.info("Housekeeping encrypted files");
        HousekeepFiles housekeepFiles = new HousekeepFiles();
        PlainJobExecutionContext context = new PlainJobExecutionContext();
        context.put("extractsToProcess", extractsToProcess);
        housekeepFiles.execute(context);
    }

    public boolean getBuildCohortDone() {
        return this.buildCohortDone;
    }

    public void setBuildCohortDone(boolean buildCohortDone) {
        this.buildCohortDone = buildCohortDone;
    }

    public boolean getGenerateFilesDone() {
        return this.generateFilesDone;
    }

    public void setGenerateFilesDone(boolean generateFilesDone) {
        this.generateFilesDone = generateFilesDone;
    }

    public void setTotalExtracts(int totalExtracts) {
        this.totalExtracts = totalExtracts;
    }

    public int incrememtZipProcessed() {
        this.zipProcessed++;
        return this.zipProcessed;
    }

    public int incrememtEncryptProcessed() {
        this.encryptProcessed++;
        return this.encryptProcessed;
    }

    public int incrememtSftpProcessed() {
        this.sftpProcessed++;
        return this.sftpProcessed;
    }

    public int incrememtExtractsProcessed() {
        this.extractsProcessed++;
        return this.extractsProcessed;
    }

    public int incrememtErrorCount() {
        this.errorCount++;
        return this.errorCount;
    }
}