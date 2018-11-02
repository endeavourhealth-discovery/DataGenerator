package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendCsvFilesSFTP implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(SendCsvFilesSFTP.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Sending Encrypted CSV files");

        //TODO Send encrypted CSV files via SFTP

        System.out.println("CSV files sent");
    }
}
