package org.endeavourhealth.scheduler;

import org.endeavourhealth.scheduler.models.database.ExtractEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            // Run the whole process
            generateData();
            return;
        }

        String step = args[0];

        if (step.equals("buildCohort")) {
            buildCohort();
        }

        if (step.equals("getData")) {
            getData();
        }

        if (step.equals("extractSQL")) {
            extractSQLtoCSV();
        }

        if (step.equals("encryptCSV")) {
            encryptCSVFiles();
        }

        if (step.equals("moveCSV")) {
            moveCSVtoSFTP();
        }

        LOG.info("Checking for extractions");
        System.out.println("generating cohorts");

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
        buildCohort();
        getData();
        extractSQLtoCSV();
        encryptCSVFiles();
        moveCSVtoSFTP();
    }

    public static void buildCohort() throws Exception {
        List<ExtractEntity> extractsToProcess = ExtractEntity.getAllExtracts();

        if (extractsToProcess.size() == 0) {
            System.out.println("No extracts to process. Exiting");
            return;
        }
        System.out.println("The following extracts have been found");
        for (ExtractEntity extract : extractsToProcess) {
            System.out.println(extract.getExtractName());
            System.out.println("Calling Darren's cohort generator code");
            // Call the Cohort Generator code
        }
    }

    private static void getData() throws Exception {
        System.out.println("Running the extracts of the data into new SQL tables");
    }

    private static void extractSQLtoCSV() throws Exception {
        System.out.println("Extracting the new SQL files to CSV");
    }

    private static void encryptCSVFiles() throws Exception {
        System.out.println("Encrypting the CSV files");
    }

    private static void moveCSVtoSFTP() throws Exception {
        System.out.println("Moving the csv files into SFTP");
    }

}
