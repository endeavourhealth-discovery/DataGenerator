package org.endeavourhealth.scheduler.job;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtractSQLToCsv implements Job {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractSQLToCsv.class);

    public void execute(JobExecutionContext jobExecutionContext) {

        System.out.println("Extract Temp tables to CSV");

        //TODO Dump Temp tables to CSV

        System.out.println("CSV files generated");
    }
}
