package org.endeavourhealth.scheduler;

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
    private static Scheduler extractSQLtoCSVScheduler;
    private static Scheduler zipCSVFilesScheduler;
    private static Scheduler encryptCSVFilesScheduler;
    private static Scheduler moveCSVtoSFTPScheduler;
    private static Scheduler housekeepFilesScheduler;

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            // Run the whole process
            generateData();
            return;

        }

        String step = args[0];

        if (step.equals("buildCohort")) {
            buildCohort(false);
        }

        if (step.equals("getData")) {
            boolean limitCols = false;
            if (args.length == 2) {
                if (args[1].equals("limit")) {
                    limitCols = true;
                }
            }
            getData(false, limitCols);
        }

        if (step.equals("extractSQL")) {
            extractSQLtoCSV(false);
        }

        if (step.equals("zipCSV")){
            zipCSVFiles(false);
        }

        if (step.equals("encryptCSV")) {
            encryptCSVFiles(false);
        }

        if (step.equals("moveCSV")) {
            moveCSVtoSFTP(false);
        }

        if (step.equals("housekeepCSV")) {
            housekeepCSV(false);
        }

        LOG.info("Checking for extractions");
        System.out.println("Generating cohorts");

        // Get list of extracts that we need to run
        // Call Darren's code to generate the cohorts
        // Figure out when that job has been completed...timed task that checks a SQL table every 30 seconds?
        // Run the data extractor SQL to move the data into new temporary tables
        // Export the SQL into CSV
        // Encrypt the CSV files
        // Push CSV files into SFTP

    }

    private static void generateData() throws Exception {
        System.out.println("Running full process");
        buildCohort(true);
        getData(true, false);
        extractSQLtoCSV(true);
        encryptCSVFiles(true);
        moveCSVtoSFTP(true);
        housekeepCSV(true);

        //TODO implementation needed to determine if everything is done
        //TODO testing job scheduling for 100s
        Thread.sleep(100000);
        if (buildCohortScheduler != null) {
            buildCohortScheduler.shutdown();
        }
        if (generateDataScheduler != null) {
            generateDataScheduler.shutdown();
        }
        if (extractSQLtoCSVScheduler != null) {
            extractSQLtoCSVScheduler.shutdown();
        }
        if (encryptCSVFilesScheduler != null) {
            encryptCSVFilesScheduler.shutdown();
        }
        if (moveCSVtoSFTPScheduler != null) {
            moveCSVtoSFTPScheduler.shutdown();
        }
        if (housekeepFilesScheduler != null) {
            housekeepFilesScheduler.shutdown();
        }
    }

    public static void buildCohort(boolean isScheduled) throws Exception {
        List<ExtractEntity> extractsToProcess = ExtractEntity.getAllExtracts();
        extractsToProcess.add(new ExtractEntity());

        if (extractsToProcess.size() == 0) {
            System.out.println("No extracts to process. Exiting");
            return;
        }
        System.out.println("The following extracts have been found");
        for (ExtractEntity extract : extractsToProcess) {
            System.out.println(extract.getExtractName());
            System.out.println("Calling Darren's cohort generator code");
            // Call the Cohort Generator code

            if (isScheduled) {
                JobDetail buildCohortJob = JobBuilder.newJob(BuildCohort.class).build();

                //TODO determine timing
                //TODO temporarily run job every 5 seconds
                Trigger buildCohortTrigger = TriggerBuilder.newTrigger()
                        .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
                        .build();

                buildCohortScheduler = new StdSchedulerFactory().getScheduler();
                buildCohortScheduler.start();
                buildCohortScheduler.scheduleJob(buildCohortJob, buildCohortTrigger);

            } else {
                BuildCohort buildCohort = new BuildCohort();
                buildCohort.execute(null);
            }
        }
    }

    private static void getData(boolean isScheduled, boolean limitCols) throws Exception {
        System.out.println("Running the extracts of the data into new SQL tables");
        if (isScheduled) {
            JobDetail generateDataJob = JobBuilder.newJob(GenerateData.class).build();

            //TODO determine timing
            //TODO temporarily run job every 10 seconds
            Trigger generateDataTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?"))
                    .build();

            generateDataScheduler = new StdSchedulerFactory().getScheduler();
            generateDataScheduler.start();
            generateDataScheduler.scheduleJob(generateDataJob, generateDataTrigger);

        } else {
            GenerateData generateData = new GenerateData();
            generateData.setLimitCols(limitCols);
            generateData.execute(null);
        }
    }

    private static void extractSQLtoCSV(boolean isScheduled) throws Exception {
        System.out.println("Extracting the new SQL files to CSV");
		if (isScheduled) {
            JobDetail extractSQLtoCSVJob = JobBuilder.newJob(ExtractSQLToCsv.class).build();

            //TODO determine timing
            //TODO temporarily run job every 15 seconds
            Trigger extractSQLtoCSVTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/15 * * * * ?"))
                    .build();

            extractSQLtoCSVScheduler = new StdSchedulerFactory().getScheduler();
            extractSQLtoCSVScheduler.start();
            extractSQLtoCSVScheduler.scheduleJob(extractSQLtoCSVJob, extractSQLtoCSVTrigger);

        } else {
            ExtractSQLToCsv extractSQLToCsv = new ExtractSQLToCsv();
            extractSQLToCsv.execute(null);
        }
    }

    private static void zipCSVFiles(boolean isScheduled) throws Exception {
        System.out.println("Zipping the CSV files");
        if (isScheduled) {
            JobDetail zipCSVFilesJob = JobBuilder.newJob(ZipCsvFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 20 seconds
            Trigger zipCSVFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/20 * * * * ?"))
                    .build();

            zipCSVFilesScheduler = new StdSchedulerFactory().getScheduler();
            zipCSVFilesScheduler.start();
            zipCSVFilesScheduler.scheduleJob(zipCSVFilesJob, zipCSVFilesTrigger);

        } else {
            ZipCsvFiles zipCsvFiles = new ZipCsvFiles();
            zipCsvFiles.execute(null);
        }
    }

    private static void encryptCSVFiles(boolean isScheduled) throws Exception {
        System.out.println("Encrypting the CSV files");
        if (isScheduled) {
            JobDetail encryptCSVFilesJob = JobBuilder.newJob(EncryptCsvFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 20 seconds
            Trigger encryptCSVFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/20 * * * * ?"))
                    .build();

            encryptCSVFilesScheduler = new StdSchedulerFactory().getScheduler();
            encryptCSVFilesScheduler.start();
            encryptCSVFilesScheduler.scheduleJob(encryptCSVFilesJob, encryptCSVFilesTrigger);

        } else {
            EncryptCsvFiles encryptCsvFiles = new EncryptCsvFiles();
            encryptCsvFiles.execute(null);
        }
    }

    private static void moveCSVtoSFTP(boolean isScheduled) throws Exception {
        System.out.println("Transferring encrypted files to SFTP");
        if (isScheduled) {
            JobDetail moveCSVtoSFTPJob = JobBuilder.newJob(TransferEncryptedFilesToSftp.class).build();

            //TODO determine timing
            //TODO temporarily run job every 25 seconds
            Trigger moveCSVtoSFTPTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/25 * * * * ?"))
                    .build();

            moveCSVtoSFTPScheduler = new StdSchedulerFactory().getScheduler();
            moveCSVtoSFTPScheduler.start();
            moveCSVtoSFTPScheduler.scheduleJob(moveCSVtoSFTPJob, moveCSVtoSFTPTrigger);

        } else {
            TransferEncryptedFilesToSftp sendCsvFilesSFTP = new TransferEncryptedFilesToSftp();
            sendCsvFilesSFTP.execute(null);
        }
    }

    private static void housekeepCSV(boolean isScheduled) throws Exception {
        System.out.println("Housekeeping encrypted files");
        if (isScheduled) {
            JobDetail housekeepFilesJob = JobBuilder.newJob(HousekeepFiles.class).build();

            //TODO determine timing
            //TODO temporarily run job every 35 seconds
            Trigger housekeepFilesTrigger = TriggerBuilder.newTrigger()
                    .withSchedule(CronScheduleBuilder.cronSchedule("0/35 * * * * ?"))
                    .build();

            housekeepFilesScheduler = new StdSchedulerFactory().getScheduler();
            housekeepFilesScheduler.start();
            housekeepFilesScheduler.scheduleJob(housekeepFilesJob, housekeepFilesTrigger);

        } else {
            HousekeepFiles housekeepFiles = new HousekeepFiles();
            housekeepFiles.execute(null);
        }
    }
}
