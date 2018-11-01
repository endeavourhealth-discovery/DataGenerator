package org.endeavourhealth.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
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

}
