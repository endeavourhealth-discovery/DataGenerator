package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptCsvFiles implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(EncryptCsvFiles.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Encrypting CSV files");

        //TODO Encrypt the CSV files

        System.out.println("CSV files encrypted");
    }
}
